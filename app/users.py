
from app.common.cache import leaderboards
from app.common.database import users

import hashlib
import app

def change_country(user_id: int, new_country: str) -> None:
    """Change the country of a user and update their ranks"""
    app.session.logger.info(f'[users] -> Changing country of user to "{new_country}"...')

    with app.session.database.managed_session() as session:
        user = users.fetch_by_id(user_id, session=session)

        if not user:
            app.session.logger.warning(f'[users] -> User "{user_id}" was not found.')
            return

        user.stats.sort(key=lambda x: x.mode)
        old_country = user.country

        leaderboards.remove_country(
            user.id,
            old_country
        )

        users.update(user.id, {'country': new_country}, session=session)
        user.country = new_country

        for mode in range(4):
            leaderboards.update(
                user.stats[mode],
                user.country
            )

    app.session.logger.info(f'[users] -> Done.')

def avatar_migration() -> None:
    with app.session.database.managed_session() as session:
        for user in users.fetch_all(session=session):
            if user.avatar_hash:
                continue
            
            avatar = app.session.storage.get_avatar(user.id)
            
            if not avatar:
                continue
        
            users.update(
                user.id,
                {'avatar_hash': hashlib.md5(avatar).hexdigest()},
                session=session
            )
