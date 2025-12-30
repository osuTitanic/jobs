
from .common.cache.events import EventQueue
from .common.database import Postgres
from .common.storage import Storage
from .common.config import Config

from requests import Session
from redis import Redis

import logging

config = Config()
database = Postgres(config)
storage = Storage(config)

redis = Redis(
    config.REDIS_HOST,
    config.REDIS_PORT
)
events = EventQueue(
    name='bancho:events',
    connection=redis
)

logger = logging.getLogger('jobs')
requests = Session()
requests.headers = {
    'User-Agent': f'osuTitanic ({config.DOMAIN_NAME})'
}
