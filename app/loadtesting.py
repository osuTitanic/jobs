# Config generator for:
# https://github.com/Lekuruu/bancho-loadtesting

from app.common.database import users, messages, names
from copy import deepcopy

import json
import app
import os

BASE_CONFIGURATION: dict = {
    "Connection": {
        "Domain": "localhost",
        "Workers": 5,
        "Version": 20241030,
        "ExecutableHash": "8be5ec016f276afebd08444febf5028e",
        "TCP": {
            "Enabled": False,
            "IP": "127.0.0.1",
            "Port": 13381
        }
    },
    "Flags": {
        "EnableMessages": True,
        "EnableStatusUpdates": True,
        "EnableSpectating": False,
        "EnableLeaderboardRequests": False
    },
    "Leaderboard": {
        "BeatmapFilename": "xi - FREEDOM DiVE (Nakagawa-Kanon) [Ono's Taiko Oni].osu",
        "BeatmapHash": "af64a0c336dfd9190e081ef8ff370629",
        "BeatmapsetId": 39804,
        "RequestIntervalMs": 1000
    },
    "Users": []
}

def generate_loadtesting_configuration(
    config_amount: int = 1,
    output_directory: str = ".data/configurations/",
    user_password: str = "test",
    spectator_target_id: int = 2,
    start_user_id: int = 4
) -> None:
    result_configs = [
        deepcopy(BASE_CONFIGURATION)
        for _ in range(config_amount)
    ]
    os.makedirs(output_directory, exist_ok=True)
    
    app.session.logger.info(
        f"[loadtesting] -> Generating {config_amount} loadtesting configurations..."
    )

    with app.session.database.managed_session() as session:
        target_users = users.fetch_all(session=session)

        if not target_users:
            app.session.logger.warning("[loadtesting] -> No users found in database.")
            return

        app.session.logger.info(
            f"[loadtesting] -> Fetched {len(target_users)} users from database."
        )

        for index, user in enumerate(target_users):
            if user.id < start_user_id:
                continue

            config_index = (index + start_user_id - 4) % config_amount
            target_config = result_configs[config_index]

            user_entry = {
                "Username": user.name,
                "Password": user_password,
                "SpectatorTargetId": spectator_target_id,
                "MessageTargetChannel": "#osu",
                "Messages": set()
            }

            name_history = names.fetch_all(user.id, session)
            target_names = set([user.name])
            target_names.update(entry.name for entry in name_history)

            for name in target_names:
                sender_messages = messages.fetch_all_by_sender(name, session)
                user_entry["Messages"].update([msg.message for msg in sender_messages])

            user_entry["Messages"] = list(user_entry["Messages"])
            target_config["Users"].append(user_entry)

            app.session.logger.info(
                f"[loadtesting] -> Added user '{user.name}' ({user.id}) with "
                f"{len(user_entry['Messages'])} messages to configuration {config_index}."
            )

    for config_index, config_data in enumerate(result_configs):
        output_path = os.path.join(
            output_directory,
            f"config_{config_index + 1}.json"
        )

        with open(output_path, "w", encoding="utf-8") as config_file:
            json.dump(config_data, config_file, indent=4)

        app.session.logger.info(
            f"[loadtesting] -> Saved configuration {config_index} to '{output_path}'."
        )
