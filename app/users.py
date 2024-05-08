
from app.common.cache import leaderboards
from app.common.database import users, scores
from app.common.database import DBScore
from collections import defaultdict

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

def recalculate_score_status(user_id: int) -> None:
    """Recalculate the score status of a user"""
    app.session.logger.info(f'[users] -> Recalculating score statuses of user...')

    with app.session.database.managed_session() as session:
        user = users.fetch_by_id(user_id, session=session)

        if not user:
            app.session.logger.warning(f'[users] -> User "{user_id}" was not found.')
            return

        user_scores = session.query(DBScore) \
            .filter(DBScore.user_id == user.id) \
            .filter(DBScore.status > 1) \
            .all()

        if not user_scores:
            app.session.logger.warning(f'[users] -> User "{user_id}" has no scores.')
            return

        # Sort scores by beatmap id
        scores_dict = defaultdict(list)

        for score in user_scores:
            scores_dict[score.beatmap_id].append(score)

        for beatmap_id, beatmap_scores in scores_dict.items():
            # Sort scores by pp
            beatmap_scores.sort(key=lambda x: x.pp, reverse=True)

            # Update best score
            best_score = beatmap_scores[0]
            scores.update(best_score.id, {'status': 3}, session=session)

            app.session.logger.info(f'[users] ({beatmap_id}) -> Best score: {best_score.pp}pp')

            # Sort scores by mods
            mods_dict = defaultdict(list)

            for score in beatmap_scores:
                mods_dict[score.mods].append(score)

            for mods, scores_list in mods_dict.items():
                # Sort scores by pp
                scores_list.sort(key=lambda x: x.pp, reverse=True)

                # Get best score with mods
                mods_best_score = scores_list.pop(0)

                # Update other scores to submitted status
                for score in scores_list:
                    best_score_ids = (mods_best_score.id, best_score.id)

                    if score.id in best_score_ids:
                        continue

                    scores.update(score.id, {'status': 2}, session=session)

                if mods == best_score.mods:
                    # Don't update the best score
                    continue

                # Update best mod-score
                scores.update(mods_best_score.id, {'status': 4}, session=session)

    app.session.logger.info(f'[users] -> Done.')
