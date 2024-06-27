
from app.common.constants import DatabaseStatus, ScoreStatus
from app.common.database import DBBeatmapset
from app.common.database import (
    nominations,
    beatmapsets,
    beatmaps,
    scores,
    topics,
    posts
)

from datetime import datetime, timedelta
from sqlalchemy.orm import Session

import app

def move_beatmap_topic(beatmapset: DBBeatmapset, status: int, session: Session):
    if not beatmapset.topic_id:
        return

    if status > DatabaseStatus.Pending:
        topics.update(
            beatmapset.topic_id,
            {'forum_id': 8},
            session=session
        )
        posts.update_by_topic(
            beatmapset.topic_id,
            {'forum_id': 8},
            session=session
        )

    elif status == DatabaseStatus.WIP:
        topics.update(
            beatmapset.topic_id,
            {'forum_id': 10},
            session=session
        )
        posts.update_by_topic(
            beatmapset.topic_id,
            {'forum_id': 10},
            session=session
        )

    elif status == DatabaseStatus.Graveyard:
        topics.update(
            beatmapset.topic_id,
            {'forum_id': 12},
            session=session
        )
        posts.update_by_topic(
            beatmapset.topic_id,
            {'forum_id': 12},
            session=session
        )

    else:
        topics.update(
            beatmapset.topic_id,
            {'forum_id': 9},
            session=session
        )
        posts.update_by_topic(
            beatmapset.topic_id,
            {'forum_id': 9},
            session=session
        )

def update_beatmap_icon(
    beatmapset: DBBeatmapset,
    status: int,
    previous_status: int,
    session: Session
) -> None:
    if status in (DatabaseStatus.Ranked, DatabaseStatus.Qualified, DatabaseStatus.Loved):
        # Set icon to heart
        topics.update(
            beatmapset.topic_id,
            {'icon_id': 1},
            session=session
        )
        return

    if status == DatabaseStatus.Approved:
        # Set icon to flame
        topics.update(
            beatmapset.topic_id,
            {'icon_id': 5},
            session=session
        )
        return

    ranked_statuses = (
        DatabaseStatus.Qualified,
        DatabaseStatus.Approved,
        DatabaseStatus.Ranked,
        DatabaseStatus.Loved
    )

    if previous_status in ranked_statuses:
        # Set icon to broken heart
        topics.update(
            beatmapset.topic_id,
            {'icon_id': 2},
            session=session
        )
        return

    if status == DatabaseStatus.Graveyard:
        has_nomination = nominations.fetch_by_beatmapset(
            beatmapset.id,
            session=session
        )

        if has_nomination:
            # Pop the bubble
            topics.update(
                beatmapset.topic_id,
                {'icon_id': 4},
                session=session
            )
            return

    # Remove icon
    topics.update(
        beatmapset.topic_id,
        {'icon_id': None},
        session=session
    )

def handle_qualified_set(beatmapset: DBBeatmapset, session: Session):
    approved_time = datetime.now() - beatmapset.approved_at
    ranking_time = timedelta(weeks=1)

    if approved_time < ranking_time:
        return

    for beatmap in beatmapset.beatmaps:
        # Hide previous scores
        scores.update_by_beatmap_id(
            beatmap.id,
            {'status': ScoreStatus.Hidden.value},
            session=session
        )

    update_beatmap_icon(
        beatmapset,
        DatabaseStatus.Ranked.value,
        beatmapset.status,
        session=session
    )

    move_beatmap_topic(
        beatmapset,
        DatabaseStatus.Ranked.value,
        session=session
    )

    beatmapsets.update(
        beatmapset.id,
        {'status': DatabaseStatus.Ranked.value},
        session=session
    )

    beatmaps.update_by_set_id(
        beatmapset.id,
        {'status': DatabaseStatus.Ranked.value},
        session=session
    )

    app.session.logger.info(
        f'[beatmaps] -> "{beatmapset.full_name}" was ranked.'
    )

def handle_pending_set(beatmapset: DBBeatmapset, session: Session):
    last_update = datetime.now() - beatmapset.last_update
    graveyard_time = timedelta(weeks=2)

    if last_update < graveyard_time:
        return

    update_beatmap_icon(
        beatmapset,
        DatabaseStatus.Graveyard.value,
        beatmapset.status,
        session=session
    )

    move_beatmap_topic(
        beatmapset,
        DatabaseStatus.Graveyard.value,
        session=session
    )

    beatmapsets.update(
        beatmapset.id,
        {'status': DatabaseStatus.Graveyard.value},
        session=session
    )

    beatmaps.update_by_set_id(
        beatmapset.id,
        {'status': DatabaseStatus.Graveyard.value},
        session=session
    )

    nominations.delete_all(
        beatmapset.id,
        session=session
    )

    app.session.logger.info(
        f'[beatmaps] -> "{beatmapset.full_name}" was sent to the beatmap graveyard.'
    )

def update_beatmap_statuses():
    with app.session.database.managed_session() as session:
        app.session.logger.info(
            '[beatmaps] -> Updating qualified beatmaps'
        )

        qualified_sets = beatmapsets.fetch_by_status(
            DatabaseStatus.Qualified.value,
            session=session
        )

        for beatmapset in qualified_sets:
            handle_qualified_set(
                beatmapset,
                session=session
            )

        app.session.logger.info(
            '[beatmaps] -> Updating pending beatmaps'
        )

        pending_sets = beatmapsets.fetch_by_status(
            DatabaseStatus.Pending.value,
            session=session
        )

        for beatmapset in pending_sets:
            handle_pending_set(
                beatmapset,
                session=session
            )

        wip_sets = beatmapsets.fetch_by_status(
            DatabaseStatus.WIP.value,
            session=session
        )

        for beatmapset in wip_sets:
            handle_pending_set(
                beatmapset,
                session=session
            )