
from app.common.database.objects import DBReplayHistory, DBPlayHistory
from app.common.database import users, histories
from app.common.cache import leaderboards

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

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

def fix_historical_data() -> None:
    for user in users.fetch_all():
        for mode in range(4):
            fix_replay_history_for_user(user.id, mode)
            fix_play_history_for_user(user.id, mode)

def fix_historical_data_for_user(user_id: str) -> None:
    for mode in range(4):
        fix_replay_history_for_user(int(user_id), mode)
        fix_play_history_for_user(int(user_id), mode)

def fix_replay_history_for_user(user_id: int, mode: int) -> None:
    """Ensure that there are no missing entries in the user's replay history"""
    with app.session.database.managed_session() as session:
        if not (user := users.fetch_by_id(user_id, session=session)):
            app.session.logger.warning(f'[users] -> User "{user_id}" was not found.')
            return

        app.session.logger.info(f'[users] -> Fixing replay history data for user "{user.name}"...')
        all_replay_entries = histories.fetch_replay_history_all(user.id, mode, session=session)
        all_replay_entries.sort(key=lambda x: (x.year, x.month))

        if not all_replay_entries:
            app.session.logger.warning(f'[users] -> No replay history found for user "{user.name}".')
            return

        last_entry: DBReplayHistory = all_replay_entries[0]

        for entry in all_replay_entries[1:]:
            last_entry_date = datetime(year=last_entry.year, month=last_entry.month, day=1)
            this_entry_date = datetime(year=entry.year, month=entry.month, day=1)

            # If the gap is more than 1 month, fill in missing months
            while last_entry_date + relativedelta(months=1) < this_entry_date:
                last_entry_date += relativedelta(months=1)

                # Create a new empty replay entry
                missing_entry = DBReplayHistory(user.id, mode)
                missing_entry.year = last_entry_date.year
                missing_entry.month = last_entry_date.month
                session.add(missing_entry)

                app.session.logger.info(
                    f"[users] -> Added missing replay history entry for "
                    f"'{user.name}' ({last_entry_date.year}-{last_entry_date.month:02d})"
                )

            session.commit()
            last_entry = entry

def fix_play_history_for_user(user_id: int, mode: int) -> None:
    """Ensure that there are no missing entries in the user's play history"""
    with app.session.database.managed_session() as session:
        if not (user := users.fetch_by_id(user_id, session=session)):
            app.session.logger.warning(f'[users] -> User "{user_id}" was not found.')
            return

        app.session.logger.info(f'[users] -> Fixing play history data for user "{user.name}"...')
        all_play_entries = histories.fetch_plays_history_all(user.id, mode, session=session)
        all_play_entries.sort(key=lambda x: (x.year, x.month))

        if not all_play_entries:
            app.session.logger.warning(f'[users] -> No play history found for user "{user.name}".')
            return

        last_entry: DBPlayHistory = all_play_entries[0]

        for entry in all_play_entries[1:]:
            last_entry_date = datetime(year=last_entry.year, month=last_entry.month, day=1)
            this_entry_date = datetime(year=entry.year, month=entry.month, day=1)

            # If the gap is more than 1 month, fill in missing months
            while last_entry_date + relativedelta(months=1) < this_entry_date:
                last_entry_date += relativedelta(months=1)

                # Create a new empty play entry
                missing_entry = DBPlayHistory(user.id, mode)
                missing_entry.year = last_entry_date.year
                missing_entry.month = last_entry_date.month
                session.add(missing_entry)
                session.commit()

                app.session.logger.info(
                    f"[users] -> Added missing play history entry for "
                    f"'{user.name}' ({last_entry_date.year}-{last_entry_date.month:02d})"
                )

            last_entry = entry
