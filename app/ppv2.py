
from app.common.database.repositories import users, scores, stats, histories
from app.common.database import DBScore, DBUser
from app.common.helpers import performance
from app.common.cache import leaderboards
from typing import List

import config
import math
import app

def calculate_weighted_pp(scores: List[DBScore]) -> float:
    if not scores:
        return 0

    weighted_pp = sum(score.pp * 0.95**index for index, score in enumerate(scores))
    bonus_pp = 416.6667 * (1 - 0.9994 ** len(scores))
    return weighted_pp + bonus_pp

def recalculate_user_scores(user: DBUser, session):
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

        if best_scores:
            rx_scores = [score for score in best_scores if (score.mods & 128) != 0]
            ap_scores = [score for score in best_scores if (score.mods & 8192) != 0]
            vn_scores = [score for score in best_scores if (score.mods & 128) == 0 and (score.mods & 8192) == 0]

            # Update performance
            user_stats.pp = calculate_weighted_pp(best_scores)
            user_stats.pp_vn = calculate_weighted_pp(vn_scores)
            user_stats.pp_rx = calculate_weighted_pp(rx_scores)
            user_stats.pp_ap = calculate_weighted_pp(ap_scores)

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
                    'pp_vn': user_stats.pp_vn,
                    'pp_rx': user_stats.pp_rx,
                    'pp_ap': user_stats.pp_ap,
                    'rank': user_stats.rank
                },
                session=session
            )

            histories.update_rank(
                user_stats,
                user.country,
                session=session
            )

        app.session.logger.info(f'[ppv2] -> Recalculated pp: {user_stats.pp}')

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
            recalculate_user_scores(
                user,
                session
            )
