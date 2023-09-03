import subprocess
import threading
import datetime
import asyncio
import signal
import json
import uuid

from starlette.websockets import WebSocketDisconnect, WebSocketState
from fastapi import FastAPI, WebSocket
import websockets
import uvicorn

from src.database import DatabaseManager, Slate
from src import logger, settings

# Initialize database manager
database = DatabaseManager()


class Client:
    """
    Epicbox client class used to manage connected wallets
    """
    def __init__(self, address: str, version: str, ws: WebSocket):
        self.ip_address = ws.client.host
        self.address = address
        self.version = version
        self.ws = ws

    def is_connected(self):
        return self.ws.client_state == WebSocketState.CONNECTED

    def _short_address(self):
        return f"{self.address[:5]}...{self.address[-5:]}" if self.address else 'Unknown'

    def __repr__(self):
        connected = f"Connected" if self.is_connected() else "Disconnected"
        return f"Client({self._short_address()} | {self.ip_address} | {connected} | ver {self.version})"

    def __str__(self):
        return self.__repr__()


class EpicboxServer:
    """
    Epicbox server class, used to manage communication workflow
    """
    path_to_epicboxlib = "./epicboxlib"
    challenge = "7WUDtkSaKyGRUnQ22rE3QUXChV8DmA6NnunDYP4vheTpc"

    def __init__(self, version: str = settings['PROTOCOL_VERSION']):
        self.version = version
        self.clients: dict[str: Client] = dict()
        self.started = datetime.datetime.utcnow()

    def uptime(self):
        return str(datetime.datetime.utcnow() - self.started).split('.')[0]

    def _run_process(self, args: list) -> str:
        """
        Helper function to manage epicboxlib RUST library
        """
        args_ = [self.path_to_epicboxlib] + args
        process = subprocess.Popen(args_, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return process.stdout.read()

    def _verify_signature(self, address: str, signature: str, message: str = None) -> bool:
        """
        Function used to validate slate's signature
        """
        if message is None:
            args = ["verifysignature", address, self.challenge, signature]
        else:
            args = ["verifysignature", address, message, signature]

        if self._run_process(args) == 'true':
            return True

        return False

    def _verify_address(self, addr_from: str, addr_to: str) -> bool:
        """
        Function used to validate slate's epicbox address
        """
        args = ["verifyaddress", addr_from, addr_to]

        if self._run_process(args) == 'true':
            return True

        return False

    @staticmethod
    def _same_server(destination) -> bool:
        """
        Helper function to check received slate's epicbox instance destination
        """
        return destination['domain'] == settings['EPICBOX_DOMAIN'] and destination['port'] == settings['EPICBOX_PORT']

    async def _prepare_post_slate(self, data: dict, challenge=None) -> tuple[dict | Slate | None, dict | None]:
        """
        Function is used to prepare transaction slate data
        """
        logger.debug('server::_prepare_post_slate() preparing slate..')

        try:
            payload = json.loads(data['str'])
            dest = payload['destination']

            destination = {
                "public_key": dest['public_key'],
                "domain": dest['domain'],
                "port": int(dest['port']) if dest['port'] else 443
                }

            # Prepare slate for the same epicbox server instance
            if self._same_server(destination):

                slate = database.create_slate(Slate(
                    made=False,
                    payload=data['str'],
                    reply_to=data['from'],
                    receiver=destination['public_key'],
                    challenge="" if challenge is None else self.challenge,
                    signature=data['signature'],
                    messageid=uuid.uuid4().hex
                    ))

            # Prepare slate for a different epicbox server instance
            else:
                slate = {
                    "type": "PostSlate",
                    "from": data['from'],
                    "to": data['to'],
                    "str": data['str'],
                    "signature": data['signature']
                    }

            return slate, destination

        except Exception as e:
            logger.error(f'server::_prepare_post_slate() failed with {str(e)}')
            return None, None

    async def _send_to_same_epicbox(self, slate: Slate) -> bool:
        """
        Function is used to send transaction slate within this epicbox instance
        """
        try:
            if slate.receiver in self.clients:
                client = self.clients[slate.receiver]
            else:
                logger.error(f"server::_send_to_same_epicbox() Receiver is invalid or not connected [{slate.receiver}]")
                return False

            sender = slate.reply_to.split('@')[0]

            if sender in self.clients:
                await self.clients[sender].ws.send_json({"type": "Ok"})
            else:
                logger.warning(f"server::_send_to_same_epicbox() Sender not connected [{sender}]")

            logger.info(f"Sending slate to the {client}")

            if client.is_connected():
                request = {
                    "type": "Slate",
                    "from": slate.reply_to,
                    "str": slate.payload,
                    "signature": slate.signature,
                    "challenge": slate.challenge,
                    }

                if client.version == settings['PROTOCOL_VERSION']:
                    request.update({"epicboxmsgid": slate.messageid, "ver": client.version})
                    await client.ws.send_json(request)

                else:
                    await client.ws.send_json(request)
                    database.delete_slate(slate)

                database.increment_analytics('slates_sent')
                logger.info(f"server::_send_to_same_epicbox() sent to {client}")
                return True

            else:
                logger.error(f"server::_send_to_same_epicbox() error sending to {client}, slate: {slate}")

        except Exception as e:
            logger.error(f'server::_send_to_same_epicbox() failed with {str(e)}')

        return False

    @staticmethod
    async def _send_to_different_epicbox(slate: dict, destination: dict) -> bool:
        """
        Function is used to send transaction slate to the other epicbox instance
        """
        server_address = f"wss://{destination['domain']}:{destination['port']}"
        logger.info(f"Sending to a different epicbox server instance [{server_address}]")

        try:
            async with websockets.connect(server_address) as ws:
                res = await ws.recv()
                response = json.loads(res)

                if response['type'] == "Challenge":
                    await ws.send(json.dumps(slate))
                    logger.info(f"server::_send_to_different_epicbox() send success")
                    database.increment_analytics('slates_sent')
                    return True
                else:
                    logger.error(f"server::_send_to_different_epicbox() error sending, response: {response}")

        except Exception as e:
            logger.error(f'server::_send_to_different_epicbox() failed with {str(e)}')

        return False

    async def post_slate(self, data: dict) -> bool:
        """
        Function is used when transaction slate is received from the wallets or other epicbox instances.
        """
        address = data['from'].split('@')[0]
        database.increment_analytics('slates_received')

        if not self._verify_address(addr_from=data['from'], addr_to=data['to']):
            logger.error(f"server::post_slate() Failed to verify addresses")
            return False

        if not self._verify_signature(address=address, message=data['str'], signature=data['signature']):
            logger.debug("server::post_slate() Failed to verify signature 1st attempt")

            if not self._verify_signature(address=address, signature=data['signature']):
                logger.error("server::post_slate() Failed to verify signature 2nd and last attempt")
                return False

        slate, destination = await self._prepare_post_slate(data)

        if self._same_server(destination):
            return await self._send_to_same_epicbox(slate)
        else:
            return await self._send_to_different_epicbox(slate, destination)

    async def subscribe(self, data: dict, ws: WebSocket) -> bool:
        """
        Function used to add active connection to the server's clients dictionary.
        Used by the wallets when epicbox listener is launched to keep the connection.
        """
        verified = False

        try:
            version = data['ver'] if 'ver' in data else settings['OLD_PROTOCOL_VERSION']

            if verified := self._verify_signature(address=data['address'], signature=data['signature']):
                ws.wallet_address = data['address']
                client = Client(address=data['address'], version=version, ws=ws)
                self.clients[client.address] = client
            else:
                logger.error(f"server::subscribe() signature verification failed")

        except Exception as e:
            logger.error(f"server::subscribe() failed with {str(e)}")

        return verified

    async def unsubscribe(self, data: dict) -> None:
        """
        Function used to disconnect the wallet from the socket
        """
        try:
            if data['address'] in self.clients:
                logger.debug(f"server::unsubscribe {self.clients[data['address']]}")
                del self.clients[data['address']]

        except Exception as e:
            logger.error(f"server::unsubscribe() failed with {str(e)}")

    async def finalized(self, data: dict) -> bool:
        """
        Function used when slate was successfully sent to the receiver, it will remove slate database record
        """
        if 'ver' in data and data['ver'] == settings['PROTOCOL_VERSION'] and "epicboxmsgid" in data:
            if not self._verify_signature(address=data['address'], signature=data['signature']):
                logger.error(f"server::finalized() signature verification failed")
                return False

            try:
                slate = database.get_slate(receiver=data['address'])
                database.delete_slate(slate)
                return True

            except Exception as e:
                logger.error(f"server::finalized() failed with {str(e)}")

        return False

    @staticmethod
    async def fast_send(ws: WebSocket):
        """
        Function used to speed-up communication, available from PROTOCOL_VER >= 2.0.0
        """
        # TODO: must be exactly 3 attempts with ~1s interval, no idea why..
        for i in range(3):
            logger.debug(f"server::fast_send() sending {i}")
            await ws.send_json({"type": "FastSend"})
            await asyncio.sleep(0.8)

    async def on_exit(self) -> None:
        logger.info(f"Terminating EpicBox server, closing active connections.")
        database.reset_analytics('active_connections')
        for _, client in self.clients.items():
            await client.ws.close()


# ================ API =================

server = EpicboxServer()
app = FastAPI()


@app.get('/')
async def home():
    """
    HTTP/S Endpoint with epicbox server homepage
    """
    stats = database.get_analytics()

    context = {
        "active_connections": stats.active_connections,
        "slates_processed": stats.slates_received + stats.slates_sent,
        "transactions": int(stats.slates_sent / 2),
        "last_update": stats.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "uptime": server.uptime()
        }

    return context


@app.websocket("/")
async def websocket_endpoint(ws: WebSocket):
    """
    Websocket endpoint used by the wallets and other epicbox instances
    """
    await ws.accept()
    ws.wallet_address = None

    logger.info(f"websocket({ws.client.host}): new connection")

    try:
        await ws.send_json({"type": "Challenge", "str": server.challenge})

        while True:
            data = await ws.receive_text()

            if 'ping' in data:
                await ws.send_text("pong")
            elif 'pong' in data:
                await ws.send_text("ping")
            else:
                data = json.loads(data)

                if 'type' not in data:
                    logger.warning(f'websocket({ws.client.host}): Wrong data received: {data}')
                    continue

                match call := data['type']:
                    case 'Challenge':
                        logger.debug(f"websocket({ws.client.host}): {call} {data}")
                        await ws.send_json({"type": "Challenge", "str": server.challenge})

                    case "Subscribe":
                        logger.debug(f"websocket({ws.client.host}): {call} {data}")
                        if await server.subscribe(data, ws):
                            await ws.send_json({"type": "Ok"})
                        else:
                            logger.error(f"websocket({ws.client.host}): {call} error")
                            await ws.send_json({"type": "Error", "kind": "signature error", "description": "Signature error"})

                    case "Unsubscribe":
                        logger.debug(f"websocket({ws.client.host}): {call} {data}")
                        await server.unsubscribe(data)
                        await ws.send_json({"type": "Ok"})

                    case "PostSlate":
                        logger.info(f"websocket({ws.client.host}): {call}")
                        if not await server.post_slate(data):
                            logger.error("PostSlate: failed to send slate")

                    case "Made":
                        logger.info(f"websocket({ws.client.host}): {call} {data}")
                        await server.finalized(data)

                    case "GetVersion":
                        logger.debug(f"websocket({ws.client.host}): {call} {data}")
                        await ws.send_json({"type": "GetVersion", "str": settings['PROTOCOL_VERSION']})

                    case "FastSend":
                        logger.debug(f"websocket({ws.client.host}): {call} {data}")
                        await server.fast_send(ws)

                    case _:
                        logger.warning(f'websocket({ws.client.host}): Wrong JSON data received: {data}')

    except WebSocketDisconnect as disc_info:
        logger.debug(f"websocket: Client disconnected: {disc_info}")
        await server.unsubscribe(data={'address': ws.wallet_address})

    except Exception as e:
        logger.error(f"End-point Exception: {str(e)}")
        await server.unsubscribe(data={'address': ws.wallet_address})


@app.on_event("startup")
async def update_connections() -> None:
    """
    Run thread updating active connections, used to send pending slates from the database
    """
    async def _run():
        logger.info(f"update_connections(): running..")

        while True:
            try:
                database.update_analytics('active_connections', len(server.clients))

                for _, client in server.clients.items():
                    slate = database.get_slate(receiver=client.address)

                    if slate and client.is_connected():
                        logger.debug(f"update_connections(): sending to {client}")

                        request = {
                            "type": "Slate",
                            "from": slate.reply_to,
                            "str": slate.payload,
                            "signature": slate.signature,
                            "challenge": slate.challenge,
                            }

                        if client.version == settings['PROTOCOL_VERSION']:
                            request.update({"epicboxmsgid": slate.messageid, "ver": client.version})
                            await client.ws.send_json(request)

                        else:
                            await client.ws.send_json(request)
                            database.delete_slate(slate)

                        database.increment_analytics('slates_sent')

                await asyncio.sleep(settings['UPDATE_INTERVAL'])

            except Exception as e:
                logger.error(f"update_connections(): {str(e)}")
                break

    loop = asyncio.get_event_loop()
    thread = threading.Thread(target=asyncio.run_coroutine_threadsafe, args=(_run(), loop), daemon=True)
    thread.start()


@app.on_event("shutdown")
def close_connections(sig=None, frame=None) -> None:
    """Close all connections on server shutdown"""
    loop = asyncio.get_event_loop()
    loop.create_task(server.on_exit())


if __name__ == "__main__":
    # Handle crl+c signal (close active socket connections)
    signal.signal(signal.SIGINT, close_connections)

    # Run uvicorn webserver
    uvicorn.run(app, host=settings['LOCAL_HOST'], port=settings['LOCAL_PORT'])
