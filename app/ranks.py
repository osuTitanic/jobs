
from app.common.database import users, stats, histories
from app.common.cache import leaderboards

import app.session

def update_ranks() -> None:
    """Update the rank history for all users"""
    app.session.logger.info('[ranks] -> Updating rank history...')

    with app.session.database.managed_session() as session:
        for user in users.fetch_all(session=session):
            for user_stats in user.stats:
                if user_stats.playcount <= 0:
                    continue

                global_rank = leaderboards.global_rank(
                    user.id,
                    user_stats.mode
                )

                if user_stats.rank != global_rank:
                    # Database rank desynced from redis
                    stats.update(
                        user.id,
                        user_stats.mode,
                        {
                            'rank': global_rank
                        },
                        session=session
                    )
                    user_stats.rank = global_rank

                    # Update rank history
                    histories.update_rank(user_stats, user.country, session=session)

            app.session.logger.info(f'[ranks] -> Updated {user.name}.')

        app.session.logger.info('[ranks] -> Done.')
