
from datetime import timedelta
from app.common.database.repositories import usercount as db_usercount
from app.common.cache import usercount as redis_usercount
from app.common.database import (
    beatmaps,
    scores,
    users
)

import app.session

def update_website_stats() -> None:
    """Update the stats required for the website"""
    with app.session.database.managed_session() as session:
        app.session.redis.set('bancho:totalusers', users.fetch_count(session=session))
        app.session.redis.set('bancho:totalbeatmaps', beatmaps.fetch_count(session=session))
        app.session.redis.set('bancho:totalscores', scores.fetch_total_count(session=session))

def update_usercount_history() -> None:
    """Update the usercount history, displayed on the website"""
    with app.session.database.managed_session() as session:
        db_usercount.create(
            count := redis_usercount.get(),
            session=session
        )
        app.session.logger.info(
            f'[usercount] -> Created usercount entry ({count} players).'
        )

        if rows := db_usercount.delete_old(timedelta(weeks=1), session=session):
            app.session.logger.info(
                f'[usercount] -> Deleted old usercount entries ({rows} rows affected).'
            )
