import logging as logger

import tomlkit


# initialize epicbox.toml settings
with open('src/epicbox.toml', 'rt', encoding="utf-8") as file:
    settings = tomlkit.load(file)

# Initialize logger and its configuration
log_file = logger.FileHandler('epicbox.log')
logger.basicConfig(format='[%(asctime)s] - %(levelname)s - %(message)s', datefmt='%H:%M:%S', level=logger.DEBUG)
logger.getLogger().addHandler(log_file)
