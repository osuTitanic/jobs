
from app.common.database.repositories import users, scores, stats
from app.common.config import config_instance as config
from app.common.database import DBScore, DBUser
from app.common.helpers import performance
from app.common.cache import leaderboards
from sqlalchemy.orm import Session
from typing import List

import multiprocessing
import math
import app
import os

def recalculate_ppv2_for_user(user: DBUser, session: Session):
    user.stats.sort(key=lambda x: x.mode)

    for user_stats in user.stats:
        best_scores = scores.fetch_best(
            user_id=user.id,
            mode=user_stats.mode,
            exclude_approved=(not config.APPROVED_MAP_REWARDS),
            session=session
        )

        app.session.logger.info(f'[ppv2] -> Updating {user.name} ({user_stats.mode}) ...')

        for score in best_scores:
            pp = performance.calculate_ppv2(score)

            if not pp:
                app.session.logger.warning(f'[ppv2] -> Failed to update pp for: {score.id}')
                continue

            score.pp = pp
            scores.update(score.id, {'pp': score.pp}, session=session)

        app.session.logger.info(f'[ppv2] -> Current pp: {user_stats.pp}')
        best_scores.sort(key=lambda x: x.pp, reverse=True)

        if best_scores:
            # Update pp & acc
            user_stats.pp = calculate_weighted_ppv2(best_scores)
            user_stats.acc = calculate_weighted_acc(best_scores)

            leaderboards.update(
                user_stats,
                user.country.lower()
            )

            user_stats.rank = leaderboards.global_rank(
                user_stats.user_id,
                user_stats.mode
            )

            stats.update(
                user.id,
                user_stats.mode,
                {
                    'pp': user_stats.pp,
                    'acc': user_stats.acc,
                    'rank': user_stats.rank,
                },
                session=session
            )

        app.session.logger.info(f'[ppv2] -> Recalculated pp: {user_stats.pp}')

def recalculate_failed_ppv2_calculations():
    with app.session.database.managed_session() as session:
        failed_scores = session.query(DBScore) \
            .filter(DBScore.pp == 0) \
            .all()
        
        for score in failed_scores:
            pp = performance.calculate_ppv2(score)

            if not pp:
                app.session.logger.warning(f'[ppv2] -> Failed to update pp for: {score.id}')
                continue

            score.pp = round(pp, 8)
            scores.update(score.id, {'pp': score.pp}, session=session)
            app.session.logger.info(f'[ppv2] -> Updated pp for: {score.id} ({score.pp})')

def recalculate_ppv2():
    with app.session.database.managed_session() as session:
        all_users = users.fetch_all(session=session)

        for user in all_users:
            user.stats.sort(key=lambda x: x.mode)

        # Sort users by their rank
        all_users.sort(
            key=lambda x: x.stats[0].rank if x.stats else math.inf
        )

        for user in all_users:
            recalculate_ppv2_for_user(
                user,
                session
            )

def recalculate_ppv2_multiprocessing(workers: int = 10) -> None:
    with app.session.database.managed_session() as session:
        app.session.logger.info(f'[ppv2] -> Updating ppv2 calculations ({workers} workers)...')

        user_list = users.fetch_all(session=session)
        user_list.sort(key=lambda user: (user.stats[0].pp if user.stats else math.inf), reverse=True)

        # Split the list into chunks
        chunk_size = math.ceil(len(user_list) / workers)
        user_chunks = list(chunks(user_list, chunk_size))

        # Create a pool of workers
        with multiprocessing.Pool(workers) as pool:
            pool.starmap(
                recalculate_ppv2_for_chunk,
                ((user_chunk,) for user_chunk in user_chunks)
            )

def recalculate_ppv2_for_chunk(users: List[DBUser]) -> None:
    # Adjust pool size
    config.POSTGRES_POOL_SIZE = 1
    config.POSTGRES_POOL_SIZE_OVERFLOW = -1
    os.environ['POSTGRES_POOL_SIZE'] = '1'
    os.environ['POSTGRES_POOL_SIZE_OVERFLOW'] = '-1'

    with app.session.database.managed_session() as session:
        app.session.logger.info(f'[ppv2] -> Updating ppv2 calculations ({len(users)} users)...')

        for user in users:
            recalculate_ppv2_for_user(
                user,
                session
            )

        app.session.logger.info(f'[ppv2] -> Done.')

def recalculate_ppv2_all_scores() -> None:
    current_index = 0
    scores_per_index = 500

    with app.session.database.managed_session() as session:
        while True:
            score_chunk = session.query(DBScore) \
                .order_by(DBScore.status_pp.desc()) \
                .offset(current_index * scores_per_index) \
                .limit(scores_per_index) \
                .all()
            
            if not score_chunk:
                break

            for score in score_chunk:
                pp = performance.calculate_ppv2(score)

                if not pp:
                    app.session.logger.warning(f'[ppv2] -> Failed to update pp for: {score.id}')
                    continue

                score.pp = pp
                scores.update(score.id, {'pp': score.pp}, session=session)

            app.session.logger.info(
                f'[ppv2] -> Recalculated chunk #{current_index} ({scores_per_index} scores).'
            )
            current_index += 1

    app.session.logger.info(f'[ppv2] -> Done.')

def calculate_weighted_ppv2(scores: List[DBScore]) -> float:
    if not scores:
        return 0

    weighted_pp = sum(score.pp * 0.95**index for index, score in enumerate(scores))
    bonus_pp = 416.6667 * (1 - 0.9994 ** len(scores))
    return weighted_pp + bonus_pp

def calculate_weighted_acc(scores: List[DBScore]) -> float:
    if not scores:
        return 0

    weighted_acc = sum(score.acc * 0.95**index for index, score in enumerate(scores))
    bonus_acc = 100.0 / (20 * (1 - 0.95 ** len(scores)))
    return (weighted_acc * bonus_acc) / 100

def chunks(list: list, amount: int):
    for i in range(0, len(list), amount):
        yield list[i:i + amount]
