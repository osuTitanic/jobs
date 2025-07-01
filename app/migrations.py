
from app.common.database.repositories import beatmaps, beatmapsets
from app.common.database import DBBeatmapset, DBBeatmap
from sqlalchemy.orm import Session, selectinload

import app.session
import zipfile
import time
import io

def fetch_beatmapset_info(set_id: int) -> dict:
    response = app.session.requests.get(f'https://osu.direct/api/v2/s/{set_id}',)
    
    if response.status_code == 200:
        return response.json()
    
    if response.status_code == 429:
        app.session.logger.warning(f"[migration] -> Rate limit hit while fetching beatmapset {set_id}. Retrying...")
        time.sleep(60)
        return fetch_beatmapset_info(set_id)

    response.raise_for_status()

def update_beatmap_metadata(info: dict, session: Session) -> None:
    beatmaps.update(
        info['id'],
        {
            'count_normal': info['count_circles'],
            'count_slider': info['count_sliders'],
            'count_spinner': info['count_spinners'],
            'drain_length': info['hit_length']
        },
        session=session
    )

def migrate_beatmaps() -> None:
    with app.session.database.managed_session() as session:
        current_offset = 0
        batch_size = 1000
        
        while True:
            beatmapsets_to_process = session.query(DBBeatmapset) \
                .options(selectinload(DBBeatmapset.beatmaps)) \
                .filter(DBBeatmapset.server == 0) \
                .offset(current_offset) \
                .limit(batch_size) \
                .all()

            if not beatmapsets_to_process:
                break

            app.session.logger.info(
                f"[migration] -> Processing batch of {len(beatmapsets_to_process)} beatmapsets..."
            )

            for beatmapset in beatmapsets_to_process:
                if not beatmapset.beatmaps:
                    continue

                beatmap = beatmapset.beatmaps[0]

                if beatmap.drain_length:
                    continue

                try:
                    info = fetch_beatmapset_info(beatmapset.id)

                    for beatmap_data in info['beatmaps']:
                        update_beatmap_metadata(beatmap_data, session)
                except Exception as e:
                    app.session.logger.error(f"[migration] -> Error processing beatmapset {beatmapset.id}: {e}")
                    
            session.commit()
            current_offset += batch_size

            app.session.logger.info(
                f"[migration] -> Completed processing {current_offset} beatmapsets."
            )

def fix_video_metadata() -> None:
    with app.session.database.managed_session() as session:
        affected_sets = session.query(DBBeatmapset) \
            .filter(DBBeatmapset.has_video == False) \
            .filter(DBBeatmapset.server == 1) \
            .all()
            
        for set in affected_sets:
            osz_file = app.session.storage.get_osz(set.id)
            
            if not osz_file:
                app.session.logger.warning(f"[migration] -> No OSZ file found for beatmapset {set.id}. Skipping...")
                continue
            
            video_file_extensions = (".wmv", ".flv", ".mp4", ".avi", ".m4v", ".mpg")
            
            with zipfile.ZipFile(io.BytesIO(b''.join(osz_file)), 'r') as zf:
                video_files = [
                    name for name in zf.namelist()
                    if name.lower().endswith(video_file_extensions)
                ]

                if len(video_files) <= 0:
                    continue

                app.session.logger.info(
                    f"[migration] -> Found {len(video_files)} video files in beatmapset {set.id}."
                )

                beatmapsets.update(
                    set.id,
                    {'has_video': True},
                    session=session
                )

        app.session.logger.info(
            f"[migration] -> Done."
        )
