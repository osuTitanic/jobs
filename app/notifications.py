
from app.common.database import messages, notifications, users
from app.common.constants import NotificationType
from sqlalchemy.orm import Session
from typing import Tuple

import app

def unread_chat_message_notifications() -> None:
    with app.session.database.managed_session() as session:
        for user in users.fetch_all(session=session):
            message, link = generate_unread_chat_notification(
                user.id,
                session
            )

            if not message:
                continue

            # Delete old chat notifications to avoid spamming
            notifications.delete_by_type(
                user.id,
                NotificationType.Chat,
                session=session
            )

            # Create new notification
            notifications.create(
                user.id,
                NotificationType.Chat,
                "New Direct Messages",
                message,
                link=link,
                session=session
            )
            
            app.session.logger.info(
                f"[notifications] -> Created unread chat notification for {user.name}"
            )

def generate_unread_chat_notification(user_id: int, session: Session) -> Tuple[str, str]:
    unread_messages = messages.fetch_dms_unread_count_all(user_id, session)
    total_messages = sum(unread_messages.values())

    if total_messages <= 0:
        return "", ""

    username_mapping = {
        user_id: users.fetch_username(user_id, session)
        for user_id in unread_messages.keys()
    }
    usernames_sorted = sorted(
        username_mapping.items(),
        key=lambda item: unread_messages[item[0]],
        reverse=True
    )
    username_list = ', '.join(username for _, username in usernames_sorted)

    if len(username_list) <= 1:
        return (
            f"You have {total_messages} unread messages from {username_list}",
            f'/account/chat?target={username_list[0][0]}'
        )

    # Replace last ", " with " and "
    last_comma_index = username_list.rfind(', ')
    username_list = (
        username_list[:last_comma_index] + ' and ' +
        username_list[last_comma_index + 2:]
    )

    return (
        f"You have {total_messages} unread messages from {username_list}",
        f'/account/chat?target={username_list[0][0]}'
    )
