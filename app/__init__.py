
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import List, Callable, Any

from . import session
from . import common

from . import beatmaps
from . import users
from . import ranks
from . import stats
from . import ppv2
from . import ppv1

import time

TASKS = [
    beatmaps.update_beatmap_statuses,
    stats.update_usercount_history,
    stats.update_website_stats,
    users.change_country,
    users.recalculate_score_status,
    ranks.update_ranks,
    ranks.index_ranks,
    ppv2.recalculate_ppv2,
    ppv1.update_ppv1,
]

@dataclass
class Task:
    function: Callable
    interval: int = 60
    args: List[Any] = field(default_factory=list)
    last_call: float = 0.0

    @property
    def name(self) -> str:
        return self.function.__name__

def run_task(task: Task) -> None:
    try:
        session.logger.info(f'[{task.name}] Running task...')
        task.last_call = time.time()
        task.function(*task.args)
    except Exception as e:
        common.officer.call(f'Failed to run task: "{e}"', exc_info=e)
    finally:
        session.logger.info(f'[{task.name}] Done. ({time.time() - task.last_call:.2f} seconds)')

def run_task_loop(tasks: List[Task]) -> None:
    session.logger.info(f'Scheduling {len(tasks)} tasks:')

    for task in tasks:
        session.logger.info(f'  - {task.name} ({task.interval})')

    executor = ThreadPoolExecutor(len(tasks))

    while True:
        for task in tasks:
            elapsed_time = (time.time() - task.last_call)

            if elapsed_time < task.interval:
                continue

            task.last_call = time.time()
            executor.submit(task.function, *task.args)

        time.sleep(1)
