
from app.common.constants import BeatmapStatus, ScoreStatus
from app.common.database import DBBeatmapset, DBBeatmap
from app.common.helpers import performance
from app.common.database.repositories import (
    nominations,
    beatmapsets,
    beatmaps,
    wrapper,
    scores,
    topics,
    posts
)

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from slider import Beatmap

import config
import app

def move_beatmap_topic(beatmapset: DBBeatmapset, status: int, session: Session):
    if not beatmapset.topic_id:
        return

    if status > BeatmapStatus.Pending:
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

    elif status == BeatmapStatus.WIP:
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

    elif status == BeatmapStatus.Graveyard:
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
    if status in (BeatmapStatus.Ranked, BeatmapStatus.Qualified, BeatmapStatus.Loved):
        # Set icon to heart
        topics.update(
            beatmapset.topic_id,
            {'icon_id': 1},
            session=session
        )
        return

    if status == BeatmapStatus.Approved:
        # Set icon to flame
        topics.update(
            beatmapset.topic_id,
            {'icon_id': 5},
            session=session
        )
        return

    ranked_statuses = (
        BeatmapStatus.Qualified,
        BeatmapStatus.Approved,
        BeatmapStatus.Ranked,
        BeatmapStatus.Loved
    )

    if previous_status in ranked_statuses:
        # Set icon to broken heart
        topics.update(
            beatmapset.topic_id,
            {'icon_id': 2},
            session=session
        )
        return

    if status == BeatmapStatus.Graveyard:
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

def hide_scores(beatmapset: DBBeatmapset, session: Session) -> None:
    for beatmap in beatmapset.beatmaps:
        # Hide previous scores
        scores.update_by_beatmap_id(
            beatmap.id,
            {
                'status_pp': ScoreStatus.Hidden.value,
                'hidden': True
            },
            session=session
        )

def handle_qualified_set(beatmapset: DBBeatmapset, session: Session):
    approved_time = datetime.now() - beatmapset.approved_at
    ranking_time = timedelta(weeks=1)

    if approved_time < ranking_time:
        return

    if config.REMOVE_SCORES_ON_RANKED:
        hide_scores(beatmapset, session=session)

    max_drain = max(
        beatmap.drain_length
        for beatmap in beatmapset.beatmaps
    )

    # Determine status based on drain time
    # Map will be set to "Approved" if drain time
    # exceeds 5 minutes, otherwise "Ranked"
    status = (
        BeatmapStatus.Ranked
        if max_drain < 5*60 else
        BeatmapStatus.Approved
    )

    update_beatmap_icon(
        beatmapset,
        status.value,
        beatmapset.status,
        session=session
    )

    move_beatmap_topic(
        beatmapset,
        status.value,
        session=session
    )

    beatmapsets.update(
        beatmapset.id,
        {
            'status': status.value,
            'approved_at': datetime.now()
        },
        session=session
    )

    beatmaps.update_by_set_id(
        beatmapset.id,
        {'status': status.value},
        session=session
    )

    app.session.logger.info(
        f'[beatmaps] -> "{beatmapset.full_name}" was {status.name.lower()}.'
    )

    # osz2 file will now be unused, so we can remove it
    app.session.storage.remove_osz2(beatmapset.id)

def handle_pending_set(beatmapset: DBBeatmapset, session: Session):
    last_update = datetime.now() - beatmapset.last_update
    graveyard_time = timedelta(weeks=2)

    if last_update < graveyard_time:
        return

    update_beatmap_icon(
        beatmapset,
        BeatmapStatus.Graveyard.value,
        beatmapset.status,
        session=session
    )

    move_beatmap_topic(
        beatmapset,
        BeatmapStatus.Graveyard.value,
        session=session
    )

    beatmapsets.update(
        beatmapset.id,
        {'status': BeatmapStatus.Graveyard.value},
        session=session
    )

    beatmaps.update_by_set_id(
        beatmapset.id,
        {'status': BeatmapStatus.Graveyard.value},
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
            BeatmapStatus.Qualified.value,
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
            BeatmapStatus.Pending.value,
            session=session
        )

        for beatmapset in pending_sets:
            handle_pending_set(
                beatmapset,
                session=session
            )

        wip_sets = beatmapsets.fetch_by_status(
            BeatmapStatus.WIP.value,
            session=session
        )

        for beatmapset in wip_sets:
            handle_pending_set(
                beatmapset,
                session=session
            )

def recalculate_beatmap_difficulty():
    with app.session.database.managed_session() as session:
        app.session.logger.info(
            '[beatmaps] -> Recalculating beatmap difficulties'
        )
        current_offset = 0

        while True:
            pending_beatmaps = session.query(DBBeatmap) \
                .offset(current_offset * 1000) \
                .limit(1000) \
                .all()
                
            if not pending_beatmaps:
                break

            for beatmap in pending_beatmaps:
                beatmap_file = app.session.storage.get_beatmap(beatmap.id)

                if not beatmap_file:
                    app.session.logger.warning(
                        f'[beatmaps] -> Beatmap file was not found! ({beatmap.id})'
                    )
                    continue

                try:
                    result = performance.calculate_difficulty(
                        beatmap_file,
                        beatmap.mode
                    )
                except Exception as e:
                    app.session.logger.warning(
                        f'[beatmaps] -> Failed to calculate difficulty for beatmap {beatmap.id}',
                        exc_info=e
                    )
                    continue

                if not result:
                    app.session.logger.warning(
                        f'[beatmaps] -> Failed to calculate difficulty for beatmap {beatmap.id}'
                    )
                    continue

                session.query(DBBeatmap) \
                    .filter(DBBeatmap.id == beatmap.id) \
                    .update({'diff': result.stars})
                session.commit()

            current_offset += 1
