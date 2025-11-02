
from app.common.constants import DatabaseStatus, ScoreStatus
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
        DatabaseStatus.Ranked
        if max_drain < 5*60 else
        DatabaseStatus.Approved
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

def update_missing_beatmap_metadata_all():
    with app.session.database.managed_session() as session:
        app.session.logger.info(
            '[beatmaps] -> Updating missing beatmapset metadata'
        )

        target_beatmaps = session.query(DBBeatmap) \
            .filter(DBBeatmap.slider_multiplier <= 0) \
            .order_by(DBBeatmap.status.desc()) \
            .all()

        app.session.logger.info(
            f'[beatmaps] -> Found {len(target_beatmaps)} beatmaps to update'
        )

        for beatmap in target_beatmaps:
            update_missing_beatmap_metadata(beatmap, session)

def update_missing_beatmap_metadata_threaded(workers: int = '10'):
    with app.session.database.managed_session() as session:
        app.session.logger.info(
            f'[beatmaps] -> Updating missing beatmapset metadata ({workers} workers)'
        )

        target_beatmaps = session.query(DBBeatmap) \
            .filter(DBBeatmap.slider_multiplier <= 0) \
            .order_by(DBBeatmap.status.desc()) \
            .all()

        app.session.logger.info(
            f'[beatmaps] -> Found {len(target_beatmaps)} beatmaps to update'
        )

    with ThreadPoolExecutor(max_workers=int(workers)) as executor:
        futures = [
            executor.submit(update_missing_beatmap_metadata, beatmap)
            for beatmap in target_beatmaps
        ]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                app.session.logger.warning(
                    f'[beatmaps] -> Failed to update beatmap metadata',
                    exc_info=e
                )

@wrapper.session_wrapper
def update_missing_beatmap_metadata(beatmap: DBBeatmap, session: Session = ...):
    beatmap_file = app.session.storage.get_beatmap(beatmap.id)

    if not beatmap_file:
        app.session.logger.warning(
            f'[beatmaps] -> Beatmap file was not found! ({beatmap.id})'
        )
        return

    try:
        slider_data = Beatmap.parse(beatmap_file.decode('utf-8', errors='ignore'))
    except Exception as e:
        app.session.logger.warning(
            f'[beatmaps] -> Failed to parse beatmap file for beatmap {beatmap.id}',
            exc_info=e
        )
        return

    session.query(DBBeatmap) \
        .filter(DBBeatmap.id == beatmap.id) \
        .update({'slider_multiplier': slider_data.slider_multiplier})
    session.commit()

    app.session.logger.info(
        f'[beatmaps] -> Updated metadata for beatmap {beatmap.id}'
    )

    if beatmap.drain_length > 0:
        return

    drain_length = calculate_beatmap_drain_length(slider_data)
    session.query(DBBeatmap) \
        .filter(DBBeatmap.id == beatmap.id) \
        .update({'drain_length': round(drain_length / 1000)})
    session.commit()

    app.session.logger.info(
        f'[beatmaps] -> Updated drain length for beatmap {beatmap.id} ({drain_length}s)'
    )

# Reference:
# https://github.com/ppy/osu/blob/master/osu.Game/Beatmaps/Timing/BreakPeriod.cs

# The minimum gap between the start of the break and the previous object.
gap_before_break = 200

# The minimum gap between the end of the break and the next object.
gap_after_break = 450

# The minimum duration required for a break to have any effect.
min_break_duration = 650

# The minimum required duration of a gap between two objects such that a break can be placed between them.
minimum_gap = gap_before_break + min_break_duration + gap_after_break

def calculate_beatmap_total_length(beatmap: Beatmap) -> int:
    """Calculate the total length of a beatmap from its hit objects"""
    hit_objects = beatmap.hit_objects()

    if len(hit_objects) <= 1:
        return 0

    last_object = hit_objects[-1].time.total_seconds() * 1000
    first_object = hit_objects[0].time.total_seconds() * 1000
    return last_object - first_object

def calculate_beatmap_drain_length(beatmap: Beatmap) -> int:
    """Calculate the drain length of a beatmap from its hit objects"""
    hit_objects = beatmap.hit_objects()

    if len(hit_objects) <= 1:
        return 0

    # Identify every break in the beatmap
    # and subtract it from the total length
    total_length = calculate_beatmap_total_length(beatmap)
    break_deltas = []

    for index, hit_object in enumerate(hit_objects):
        if index <= 0:
            continue
        
        previous_object = hit_objects[index - 1]
        delta_time = hit_object.time - previous_object.time
        delta_time_seconds = delta_time.total_seconds() * 1000
        
        if delta_time_seconds <= minimum_gap:
            continue

        break_deltas.append(delta_time_seconds - (gap_before_break + gap_after_break))
        
    total_break_time = sum(break_deltas)
    return max(total_length - total_break_time, 0)
