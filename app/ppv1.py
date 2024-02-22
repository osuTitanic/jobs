
from app.common.database import users, stats, scores, histories
from app.common.helpers import performance
from app.common.cache import leaderboards

import app.session
import config

def update_ppv1() -> None:
    """Update ppv1 calculations for all users"""
    app.session.logger.info('[ppv1] -> Updating ppv1 calculations...')

    with app.session.database.managed_session() as session:
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

                user_stats.ppv1 = performance.calculate_weighted_ppv1(best_scores)

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

            app.session.logger.info(f'[ppv1] -> Updated {user.username} ({user.id}).')

    app.session.logger.info('[ppv1] -> Done.')
