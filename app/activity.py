
from app.common.constants import UserActivity
from app.common.database import DBActivity
from sqlalchemy.orm import Session

import app
import re

RANKS_GAINED_REGEX = re.compile(
    r'{} has risen (?P<ranks_gained>\d+) rank[s]?, now placed #(?P<rank>\d+) overall in (?P<mode_name>.+)\.'
)

NUMBER_ONE_REGEX = re.compile(
    r'{} has taken the lead as the top-ranked (?P<mode_name>.+) player\.'
)

BEATMAP_LEADERBOARD_RANK_REGEX = re.compile(
    r'{} achieved rank #(?P<rank>\d+) on {}(?: with (?P<mods>.+?))? <(?P<mode>.+)>(?: \((?P<pp>\d+)pp\))?'
)

LOST_FIRST_PLACE_REGEX = re.compile(
    r'{} has lost first place on {} (?P<mode_name>.+)'
)

PP_RECORD_REGEX = re.compile(
    r'{} has set the new pp record on {} with (?P<pp>\d+)pp (?P<mode_name>.+)'
)

TOP_PLAY_REGEX = re.compile(
    r'{} got a new top play on {} with (?P<pp>\d+)pp (?P<mode_name>.+)'
)

ACHIEVEMENT_UNLOCKED_REGEX = re.compile(
    r'{} unlocked an achievement: (?P<achievement_name>.+)'
)

def migrate_activity() -> None:
    iteration_size = 1000
    iteration_offset = 0

    with app.session.database.managed_session() as session:
        while True:
            activities = session.query(DBActivity) \
                .offset(iteration_offset) \
                .limit(iteration_size) \
                .all()

            if not activities:
                break

            for activity in activities:
                apply_migration(activity, session)

            session.commit()
            iteration_offset += iteration_size

def apply_migration(activity: DBActivity, session: Session) -> None:
    if activity.type == 0:
        return

    activity_type = UserActivity(activity.type)
    activity_args = (activity.activity_args or '').split('||')
    activity_links = (activity.activity_links or '').split('||')
    data = {}

    match activity_type:
        case UserActivity.RanksGained:
            # ... has risen <ranks_gained> ranks, now placed #<rank> overall in <mode_name>.
            match = RANKS_GAINED_REGEX.match(activity.activity_text)
            
            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid RanksGained description: "{activity.activity_text}" for user {activity.user_id}'
                )

            data['username'] = activity_args[0]
            data['mode'] = match.group('mode_name')
            data['rank'] = int(match.group('rank'))
            data['ranks_gained'] = int(match.group('ranks_gained'))

        case UserActivity.NumberOne:
            # ... has taken the lead as the top-ranked <mode_name> player.
            match = NUMBER_ONE_REGEX.match(activity.activity_text)

            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid NumberOne description: "{activity.activity_text}" for user {activity.user_id}'
                )

            data['username'] = activity_args[0]
            data['mode'] = match.group('mode_name')

        case UserActivity.BeatmapLeaderboardRank:
            # ... achieved rank #<beatmap_rank> on <beatmap> (with <mods>) <<mode>> (<pp>pp)
            match = BEATMAP_LEADERBOARD_RANK_REGEX.match(activity.activity_text)

            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid BeatmapLeaderboardRank description: "{activity.activity_text}" for user {activity.user_id}'
                )

            data['beatmap_rank'] = int(match.group('rank'))
            data['beatmap_id'] = int(activity_links[-1].split('/')[-1])
            data['beatmap'] = activity_args[1]
            data['username'] = activity_args[0]
            data['mode'] = match.group('mode')
            data['mods'] = match.group('mods')
            
            if match.group('pp'):
                data['pp'] = int(match.group('pp'))

        case UserActivity.LostFirstPlace:
            # ... has lost first place on <beatmap> <mode_name>
            match = LOST_FIRST_PLACE_REGEX.match(activity.activity_text)

            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid LostFirstPlace description: "{activity.activity_text}" for user {activity.user_id}'
                )

            data['mode'] = match.group('mode_name')
            data['username'] = activity_args[0]
            data['beatmap'] = activity_args[-1]
            data['beatmap_id'] = int(activity_links[-1].split('/')[-1])

        case UserActivity.PPRecord:
            # ... has set the new pp record on {} with <pp>pp <mode_name>
            match = PP_RECORD_REGEX.match(activity.activity_text)
            
            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid PPRecord description: "{activity.activity_text}" for user {activity.user_id}'
                )
                
            data['beatmap_id'] = int(activity_links[-1].split('/')[-1])
            data['beatmap'] = activity_args[-1]
            data['username'] = activity_args[0]
            data['mode'] = match.group('mode_name')
            data['pp'] = int(match.group('pp'))

        case UserActivity.TopPlay:
            # ... got a new top play on {} with <pp>pp <mode_name>
            match = TOP_PLAY_REGEX.match(activity.activity_text)

            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid TopPlay description: "{activity.activity_text}" for user {activity.user_id}'
                )

            data['beatmap_id'] = int(activity_links[-1].split('/')[-1])
            data['beatmap'] = activity_args[-1]
            data['username'] = activity_args[0]
            data['mode'] = match.group('mode_name')
            data['pp'] = int(match.group('pp'))
            
        case UserActivity.AchievementUnlocked:
            # ... unlocked an achievement: <achievement_name>
            match = ACHIEVEMENT_UNLOCKED_REGEX.match(activity.activity_text)

            if not match:
                return app.session.logger.warning(
                    f'[activity] -> Invalid AchievementUnlocked description: "{activity.activity_text}" for user {activity.user_id}'
                )

            data['username'] = activity_args[0]
            data['achievement'] = match.group('achievement_name')

        case _:
            return app.session.logger.warning(
                f'[activity] -> Unhandled activity type: {activity_type.name} for user {activity.user_id}'
            )

    session.query(DBActivity) \
        .filter(DBActivity.id == activity.id) \
        .update({'data': data})

    app.session.logger.info(
        f'[activity] -> Migrated activity {activity.id} for user {activity.user_id} with data: {data}'
    )
