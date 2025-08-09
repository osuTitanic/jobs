
from app.common.database import users, stats, histories
from app.common.cache import leaderboards

import app.session
import config

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

                peak_rank = histories.fetch_peak_global_rank(
                    user.id,
                    user_stats.mode,
                    session=session
                )

                if user_stats.rank != global_rank:
                    # Database rank desynced from redis
                    stats.update(
                        user.id,
                        user_stats.mode,
                        {'rank': global_rank},
                        session=session
                    )
                    user_stats.rank = global_rank

                    if not config.FROZEN_RANK_UPDATES:
                        # Update rank history
                        histories.update_rank(
                            user_stats,
                            user.country,
                            session=session
                        )

                if user_stats.peak_rank != peak_rank:
                    # User achieved a higher rank
                    stats.update(
                        user.id, user_stats.mode,
                        {'peak_rank': peak_rank},
                        session=session
                    )

            app.session.logger.info(f'[ranks] -> Updated {user.name}.')

        app.session.logger.info('[ranks] -> Done.')

def index_ranks() -> None:
    """Check if the redis leaderboards are empty and rebuild them if necessary"""
    if leaderboards.top_players(0):
        app.session.logger.info(f'[ranks] -> Leaderboard is not empty, please clear it first.')
        return

    app.session.logger.info(f'[ranks] -> Indexing player ranks...')

    with app.session.database.managed_session() as session:
        active_players = users.fetch_all(session=session)

        for player in active_players:
            for stats in player.stats:
                leaderboards.update(
                    stats,
                    player.country.lower()
                )
                leaderboards.update_leader_scores(
                    stats,
                    player.country.lower(),
                    session=session
                )

            leaderboards.update_kudosu(
                player.id,
                player.country.lower(),
                session=session
            )

        app.session.logger.info('[ranks] -> Done.')
