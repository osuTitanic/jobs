
from app.common.config import config_instance as config
from app.common.database.objects import DBReleaseFiles
from app.common.database.repositories import releases
from sqlalchemy.orm import Session
from typing import Iterator, List
from datetime import datetime

import requests
import app

session = requests.Session()
session.headers.update({
    "User-Agent": "osu!",
    "Accept": "application/json"
})

def release_updates(*release_streams) -> None:
    with app.session.database.managed_session() as session:
        if not config.RELEASE_UPDATES_ENABLED:
            app.session.logger.info("[releases] -> Release updates are disabled. Skipping...")
            return

        for stream in release_streams:
            app.session.logger.info(f'[releases] -> Checking for updates on stream "{stream}"...')
            updates = check_stream(stream, session)

            for created_file in updates:
                app.session.logger.info(
                    f'[releases] -> New release file created: '
                    f'{created_file.filename} / {created_file.file_version} ({created_file.file_hash})'
                )

        app.session.logger.info('[releases] -> Done.')

def check_stream(stream: str, database_session: Session) -> Iterator[DBReleaseFiles]:
    response = session.get(f"https://osu.ppy.sh/web/check-updates.php?action=check&stream={stream}")

    if not response.ok:
        app.session.logger.error(f'[releases] -> Failed to check "{stream}" for updates: {response.data} ({response.status_code})')
        return []

    data = response.json()

    if type(data) is not list:
        app.session.logger.error(f'[releases] -> Failed to check "{stream}" for updates: {data}')
        return []

    for file in data:
        file_version = int(file["file_version"])
        existing_file = releases.fetch_official_file_by_version(file_version, database_session)

        if existing_file:
            app.session.logger.debug(f'[releases] -> File with version "{file_version}" already exists. Skipping...')
            continue

        yield releases.create_official_file(
            filename=file["filename"],
            file_version=file_version,
            file_hash=file["file_hash"],
            filesize=int(file["filesize"]),
            url_full=file.get("url_full"),
            url_patch=file.get("url_patch"),
            patch_id=file.get("patch_id"),
            timestamp=datetime.strptime(file["timestamp"], '%Y-%m-%d %H:%M:%S'),
            session=database_session
        )

    database_session.commit()
