from app.common.database import users, stats, scores, histories
from app.common.helpers import performance
from app.common.cache import leaderboards
from typing import List

import multiprocessing
import app.session
import config

def update_ppv1() -> None:
    """Update ppv1 calculations for all users"""
    with app.session.database.managed_session() as session:
        app.session.logger.info('[ppv1] -> Updating ppv1 calculations...')

        for user in users.fetch_all(session=session):
            for user_stats in user.stats:
                if user_stats.playcount <= 0:
                    continue

                best_scores = scores.fetch_best(
                    user.id,
                    user_stats.mode,
                    exclude_approved=(not config.APPROVED_MAP_REWARDS),
                    session=session
                )

                if not best_scores:
                    continue

                user_stats.ppv1 = performance.recalculate_weighted_ppv1(best_scores)

                # Update stats
                stats.update(
                    user.id,
                    user_stats.mode,
                    {'ppv1': user_stats.ppv1},
                    session=session
                )

                # Update cache
                leaderboards.update(
                    user_stats,
                    user.country
                )

                # Update rank history
                histories.update_rank(
                    user_stats,
                    user.country,
                    session=session
                )

            app.session.logger.info(f'[ppv1] -> Updated {user.name} ({user.id}).')

        app.session.logger.info('[ppv1] -> Done.')

def recalculate_slice(all_scores: List[scores.DBScore]) -> None:
    with app.session.database.managed_session() as session:
        for score in all_scores:
            scores.update(
                score.id,
                {'ppv1': performance.calculate_ppv1(score)},
                session=session
            )

def recalculate_ppv1_all_scores(min_status: int = -1, workers: int = 10) -> None:
    with app.session.database.managed_session() as session:
        all_scores = session.query(scores.DBScore) \
            .filter(scores.DBScore.status_pp > min_status) \
            .order_by(scores.DBScore.id) \
            .all()

        app.session.logger.info(f'[ppv1] -> Recalculating ppv1 for {len(all_scores)} scores...')

        with multiprocessing.Pool(workers) as pool:
            pool.map(
                recalculate_slice,
                chunks(all_scores, len(all_scores) // workers)
            )

        app.session.logger.info('[ppv1] -> Done.')

def chunks(list: list, amount: int):
    for i in range(0, len(list), amount):
        yield list[i:i + amount]
