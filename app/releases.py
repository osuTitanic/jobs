
from app.common.config import config_instance as config
from app.common.database.objects import DBReleaseFiles
from app.common.database.repositories import releases
from app.common.webhooks import Webhook, Embed
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Iterator

import requests
import app

session = requests.Session()
session.headers.update({
    "User-Agent": "osu!",
    "Accept": "application/json"
})
windows_os_parameter = "10.0.0.26100.1.0"

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
                post_update_actions(created_file, stream)

        app.session.logger.info('[releases] -> Done.')

def check_stream(stream: str, database_session: Session) -> Iterator[DBReleaseFiles]:
    data_windows = fetch_stream(stream, windows_os_parameter)
    data_linux = fetch_stream(stream)
    data = data_windows + data_linux

    for file in data:
        file_version = int(file["file_version"])
        existing_file = releases.fetch_official_file_by_version(file_version, database_session)

        if existing_file:
            app.session.logger.debug(f'[releases] -> File with version "{file_version}" already exists. Skipping...')
            continue

        release = releases.create_official_file(
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
        yield release

def fetch_stream(stream: str, os: str | None = None) -> list[dict]:
    params = {
        "action": "check",
        "stream": stream
    }

    if os is not None:
        # Different versions of osu!auth.dll exist for linux & windows
        # The "os" query parameter determines which variant to retrieve
        params["os"] = os

    response = session.get(
        "https://osu.ppy.sh/web/check-updates.php",
        params=params
    )

    if not response.ok:
        app.session.logger.error(f'[releases] -> Failed to check "{stream}" for updates: {response.text} ({response.status_code})')
        return []

    data = response.json()

    if type(data) is not list:
        app.session.logger.error(f'[releases] -> Failed to check "{stream}" for updates: {data}')
        return []

    return data

def post_update_actions(file: DBReleaseFiles, stream: str) -> None:
    notify_webhook(file, stream)
    upload_to_s3(file.url_full)
    upload_to_s3(file.url_patch)

def upload_to_s3(url: str) -> None:
    if not url:
        return

    if not config.RELEASE_UPDATE_S3_TARGET:
        return

    if not app.session.storage.valid_s3_configuration:
        return

    app.session.logger.info(f'[releases] -> Downloading file from "{url}"...')
    response = session.get(url, stream=True)
    response.raw.decode_content = True

    if not response.ok:
        app.session.logger.error(f'[releases] -> Failed to download file from "{url}": {response.text} ({response.status_code})')
        return

    checksum = url.rsplit('/', 1)[-1]
    filename = url.rsplit('/', 2)[-2]
    key = f'{filename}/{checksum}'

    app.session.logger.info(f'[releases] -> Uploading file to S3 "{key}"...')
    app.session.storage.save_to_s3(response.raw, key, config.RELEASE_UPDATE_S3_TARGET)

def notify_webhook(file: DBReleaseFiles, stream: str) -> None:
    if not config.RELEASE_UPDATE_NOTIFY_WEBHOOK:
        return

    webhook = Webhook(
        url=config.RELEASE_UPDATE_NOTIFY_WEBHOOK,
        username="Release Updates",
        avatar_url="https://osu.ppy.sh/images/layout/osu-logo.png",
    )
    embed = Embed(
        title="New release file",
        description=f"A new file has been added to the `{stream}` release stream.",
        color=0xFF66AB
    )

    embed.add_field("Filename", f"`{file.filename}`", inline=True)
    embed.add_field("Version", f"`{file.file_version}`", inline=True)
    embed.add_field("File Hash", f"`{file.file_hash}`", inline=True)
    embed.add_field("Filesize", format_filesize(file.filesize), inline=True)

    if file.url_full:
        embed.add_field("Download", f"[Full]({file.url_full})", inline=True)

    if file.url_patch:
        embed.add_field("Download", f"[Patch]({file.url_patch})", inline=True)

    webhook.add_embed(embed)

    try:
        success = webhook.post()
    except Exception as e:
        app.session.logger.error(f'[releases] -> Failed to post webhook notification for "{file.filename}": {e}')
        return

    if not success:
        app.session.logger.error(
            f'[releases] -> Failed to post webhook notification for "{file.filename}"'
        )

def format_filesize(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024: return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"
