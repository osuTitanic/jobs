
from typing import List, Callable

from . import session
from . import common

from . import users
from . import ranks
from . import stats
from . import ppv2
from . import ppv1

import time

TASKS = [
    stats.update_usercount_history,
    stats.update_website_stats,
    users.change_country,
    users.recalculate_score_status,
    ranks.update_ranks,
    ranks.index_ranks,
    ppv2.recalculate_ppv2,
    ppv1.update_ppv1,
]

def run_tasks(tasks: List[Callable], *args) -> None:
    for task in tasks:
        try:
            session.logger.info(f'Running task: {task.__name__}')
            start_time = time.time()
            task(*args)
        except Exception as e:
            common.officer.call(f'Failed to run task: "{e}"', exc_info=e)
        finally:
            session.logger.info(f'Done. ({time.time() - start_time:.2f} seconds)')

def run_task_loop(tasks: List[Callable], interval_seconds: int = 60, *args) -> None:
    while True:
        run_tasks(tasks, *args)
        session.logger.info(f'Waiting {interval_seconds} seconds...')
        time.sleep(interval_seconds)
