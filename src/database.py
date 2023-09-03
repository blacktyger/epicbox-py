from datetime import datetime
from typing import Optional

from sqlmodel import SQLModel, Session, Field
from sqlalchemy import create_engine

from . import logger, settings


class Slate(SQLModel, table=True):
    """
    Slate model used to manage slate data in the database
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    made: bool = Field(default=False)
    payload: str
    receiver: str
    reply_to: str
    challenge: str = Field(default="")
    signature: str
    messageid: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class Analytics(SQLModel, table=True):
    """
    Analytics model used to manage epicbox serve instance stats data in the database
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    slates_sent: int = Field(default=0)
    slates_received: int = Field(default=0)
    active_connections: int = Field(default=0)


class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(settings['DATABASE_URI'])
        self.create_db_and_tables()
        self.create_analytics()

    def create_db_and_tables(self) -> None:
        SQLModel.metadata.create_all(self.engine)

    def create_slate(self, slate: Slate) -> Slate:
        with Session(self.engine) as session:
            session.add(slate)
            session.commit()
            session.refresh(slate)
            logger.info(f"Slate {slate.messageid} saved in the database.")
            return slate

    def get_slate(self, receiver: str)-> Slate:
        with Session(self.engine) as session:
            return session.query(Slate).filter(Slate.receiver == receiver).order_by(Slate.timestamp).first()

    def delete_slate(self, slate: Slate) -> None:
        if slate:
            with Session(self.engine) as session:
                session.delete(slate)
                session.commit()
                logger.info(f"Slate {slate.messageid} completed and deleted from the database.")

    def create_analytics(self) -> None:
        with Session(self.engine) as session:
            instance = session.query(Analytics).filter_by(id=0).first()

            if not instance:
                logger.debug(f"Creating analytics object.. ")
                instance = Analytics(id=0)
                session.add(instance)
                session.commit()
                session.refresh(instance)
                logger.debug(f"Analytics: {instance}")

    def get_analytics(self) -> Analytics | None:
        with Session(self.engine) as session:
            return session.query(Analytics).filter_by(id=0).first()

    def increment_analytics(self, field: str) -> Analytics:
        with Session(self.engine) as session:
            instance = session.query(Analytics).filter_by(id=0).first()
            value = getattr(instance, field)
            value += 1
            setattr(instance, field, value)
            setattr(instance, 'timestamp', datetime.utcnow())
            session.commit()
            session.refresh(instance)
            logger.debug(f"Analytics {field}: {getattr(instance, field)}")
            return instance

    def update_analytics(self, field: str, value: int) -> Analytics:
        with Session(self.engine) as session:
            instance = session.query(Analytics).filter_by(id=0).first()
            setattr(instance, field, value)
            setattr(instance, 'timestamp', datetime.utcnow())
            session.commit()
            session.refresh(instance)
            logger.debug(f"Analytics {field}: {getattr(instance, field)}")
            return instance

    def reset_analytics(self, field: str) -> None:
        with Session(self.engine) as session:
            instance = session.query(Analytics).filter_by(id=0).first()
            setattr(instance, field, 0)
            setattr(instance, 'timestamp', datetime.utcnow())
            session.commit()