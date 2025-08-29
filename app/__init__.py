
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Callable, Any
from threading import Thread, Event

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
    users.fix_replay_history_for_user,
    users.fix_play_history_for_user,
    users.fix_historical_data,
    scores.recalculate_pp_status,
    scores.recalculate_score_status,
    scores.recalculate_statuses_all,
    scores.recalculate_rx_scores,
    scores.oldsu_score_migration,
    scores.rx_score_migration,
    ranks.update_ranks,
    ranks.index_ranks,
    ppv2.recalculate_all_scores,
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

    def run(self) -> None:
        try:
            session.logger.info(f'[{self.name}] Running task...')
            self.last_call = time.time()
            self.function(*self.args)
        except Exception as e:
            common.officer.call(f'Failed to run task: "{e}"', exc_info=e)
        finally:
            session.logger.info(
                f'[{self.name}] Done. '
                f'({time.time() - self.last_call:.2f} seconds)'
            )

    def loop(self) -> None:
        while True:
            time.sleep(self.interval)
            self.run()

def schedule_task(task: Task) -> Thread:
    thread = Thread(target=task.loop, daemon=True)
    thread.start()
    return thread

def run_task_loop(tasks: List[Task]) -> None:
    session.logger.info(f'Scheduling {len(tasks)} tasks:')

    for task in tasks:
        session.logger.info(f'  - {task.name} ({task.interval})')
        schedule_task(task)

    # Keep the main thread alive to allow tasks to run
    Event().wait()
