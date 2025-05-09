
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import List, Callable, Any

from . import session
from . import common

from . import beatmaps
from . import scores
from . import users
from . import ranks
from . import stats
from . import ppv2
from . import ppv1

import time

TASKS = [
    beatmaps.recalculate_beatmap_difficulty,
    beatmaps.update_beatmap_statuses,
    stats.update_usercount_history,
    stats.update_website_stats,
    stats.recalculate_stats_all,
    stats.recalculate_stats,
    stats.restore_stats,
    users.change_country,
    scores.recalculate_pp_status,
    scores.recalculate_score_status,
    scores.recalculate_statuses_all,
    scores.recalculate_rx_scores,
    ranks.update_ranks,
    ranks.index_ranks,
    ppv2.recalculate_failed_ppv2_calculations,
    ppv2.recalculate_ppv2_multiprocessing,
    ppv2.recalculate_ppv2,
    ppv1.update_ppv1,
    ppv1.update_ppv1_multiprocessing,
    ppv1.recalculate_ppv1_all_scores
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
