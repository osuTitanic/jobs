
from typing import List, Callable

from . import session
from . import common

from . import ranks
from . import stats
from . import ppv1

import time

TASKS = [
    ppv1.update_ppv1,
    ranks.update_ranks,
    stats.update_usercount_history,
    stats.update_website_stats
]

def run_tasks(tasks: List[Callable]) -> None:
    for task in tasks:
        session.logger.info(f'Running task: {task.__name__}')
        task()

def run_task_loop(tasks: List[Callable], interval_seconds: int = 60) -> None:
    while True:
        run_tasks(tasks)
        session.logger.info(f'Waiting {interval_seconds} seconds...')
        time.sleep(interval_seconds)
