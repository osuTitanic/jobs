
from .common.helpers.performance import ppv2, ppv2_rosu
from .common.helpers.beatmaps import BeatmapResources
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
beatmaps = BeatmapResources(storage, redis)

logger = logging.getLogger('jobs')
requests = Session()
requests.headers = {
    'User-Agent': f'osuTitanic ({config.DOMAIN_NAME})'
}

# Initialize ppv2 calculator
instance = ppv2_rosu.RosuPerformanceCalculator(beatmaps)
ppv2.initialize_calculator(instance)
