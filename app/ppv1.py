from app.common.database import users, stats, scores, histories
from app.common.database.objects import DBUser
from app.common.helpers import performance
from app.common.cache import leaderboards
from sqlalchemy.orm import Session
from typing import List

import multiprocessing
import app.session
import config
import math
import os

def update_ppv1() -> None:
    """Update ppv1 calculations for all users"""
    with app.session.database.managed_session() as session:
        app.session.logger.info('[ppv1] -> Updating ppv1 calculations...')

        user_list = users.fetch_all(session=session)
        user_list.sort(key=lambda user: (user.stats[0].ppv1 if user.stats else math.inf), reverse=True)

        for user in user_list:
            update_ppv1_for_user(user, session)

        app.session.logger.info('[ppv1] -> Done.')

def update_ppv1_for_user(user: DBUser, session: Session) -> None:
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

        user_stats.ppv1 = performance.recalculate_weighted_ppv1(
            best_scores,
            session
        )

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

        if not config.FROZEN_RANK_UPDATES:
            # Update rank history
            histories.update_rank(
                user_stats,
                user.country,
                session=session
            )

    app.session.logger.info(f'[ppv1] -> Updated {user.name} ({user.id}).')

def update_ppv1_multiprocessing(workers: str = '10') -> None:
    with multiprocessing.Pool(int(workers)) as pool:
        with app.session.database.managed_session() as session:
            app.session.logger.info(f'[ppv1] -> Updating ppv1 calculations ({workers} workers)...')

            user_list = users.fetch_all(session=session)
            user_list.sort(key=lambda user: (user.stats[0].ppv1 if user.stats else math.inf), reverse=True)

            pool.starmap(
                update_ppv1_for_user_no_session,
                ((user,) for user in user_list)
            )

def update_ppv1_for_user_no_session(user: DBUser) -> None:
    # Reset the database connection pool
    app.session.database.engine.dispose()

    with app.session.database.managed_session() as session:
        update_ppv1_for_user(user, session)

def recalculate_slice(all_scores: List[scores.DBScore]) -> None:
    app.session.database.engine.dispose()

    with app.session.database.managed_session() as session:
        for score in all_scores:
            scores.update(
                score.id,
                {'ppv1': performance.calculate_ppv1(score)},
                session=session
            )
            app.session.logger.info(
                f'[ppv1] -> Updated ppv1 for: {score.id} ({score.ppv1})'
            )

def recalculate_ppv1_all_scores(min_status: str = '-1', workers: int = '10') -> None:
    with app.session.database.managed_session() as session:
        all_scores = session.query(scores.DBScore) \
            .filter(scores.DBScore.status_pp > int(min_status)) \
            .order_by(scores.DBScore.id) \
            .all()

        app.session.logger.info(f'[ppv1] -> Recalculating ppv1 for {len(all_scores)} scores...')

        # Adjust pool size
        config.POSTGRES_POOLSIZE = 1
        config.POSTGRES_POOLSIZE_OVERFLOW = -1
        os.environ['POSTGRES_POOLSIZE'] = '1'
        os.environ['POSTGRES_POOLSIZE_OVERFLOW'] = '-1'

        with multiprocessing.Pool(int(workers)) as pool:
            pool.map(
                recalculate_slice,
                chunks(all_scores, len(all_scores) // int(workers))
            )

        app.session.logger.info('[ppv1] -> Done.')

def chunks(list: list, amount: int):
    for i in range(0, len(list), amount):
        yield list[i:i + amount]
