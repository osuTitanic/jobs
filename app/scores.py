
from app.common.helpers.score import calculate_rx_score
from app.common.database import users, scores, beatmaps
from app.common.database.objects import DBScore
from app.common.helpers import performance
from app.common.constants import GameMode
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import or_
from datetime import datetime

import hashlib
import base64
import csv
import app

def recalculate_pp_status(user_id: int, mode: int) -> None:
    """Recalculate the pp status of a user's scores"""
    app.session.logger.info(f'[users] -> Recalculating pp statuses of user...')

    with app.session.database.managed_session() as session:
        user = users.fetch_by_id(user_id, session=session)

        if not user:
            app.session.logger.warning(f'[users] -> User "{user_id}" was not found.')
            return

        user_scores = session.query(DBScore) \
            .filter(DBScore.user_id == user.id) \
            .filter(DBScore.mode == mode) \
            .filter(DBScore.status_pp > 1) \
            .filter(DBScore.hidden == False) \
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
            scores.update(best_score.id, {'status_pp': 3}, session=session)

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

                    scores.update(score.id, {'status_pp': 2}, session=session)

                if mods == best_score.mods:
                    # Don't update the best score
                    continue

                # Update best mod-score
                scores.update(mods_best_score.id, {'status_pp': 4}, session=session)

    app.session.logger.info(f'[users] -> Done.')

def recalculate_score_status(user_id: int, mode: int) -> None:
    """Recalculate the scpre status of a user's scores"""
    with app.session.database.managed_session() as session:
        user = users.fetch_by_id(user_id, session=session)

        if not user:
            app.session.logger.warning(f'[users] -> User "{user_id}" was not found.')
            return

        # Update unmigrated scores to pp status
        session.query(DBScore) \
            .filter(DBScore.status_score == -1) \
            .filter(DBScore.status_pp > -1) \
            .filter(DBScore.hidden == False) \
            .update({'status_score': DBScore.status_pp})
        session.commit()

        # Recalculate score statuses
        user_scores = session.query(DBScore) \
            .filter(DBScore.user_id == user.id) \
            .filter(DBScore.mode == mode) \
            .filter(DBScore.status_score > 1) \
            .filter(DBScore.hidden == False) \
            .all()

        if not user_scores:
            app.session.logger.warning(f'[users] -> User "{user_id}" has no scores.')
            return

        # Sort scores by beatmap id
        scores_dict = defaultdict(list)

        for score in user_scores:
            scores_dict[score.beatmap_id].append(score)

        for beatmap_id, beatmap_scores in scores_dict.items():
            # Sort scores by total score
            beatmap_scores.sort(key=lambda x: x.total_score, reverse=True)

            # Update best score
            best_score = beatmap_scores[0]
            scores.update(best_score.id, {'status_score': 3}, session=session)

            app.session.logger.info(f'[users] <{user_id}> ({beatmap_id}) -> Best score: {best_score.total_score}')

            # Sort scores by mods
            mods_dict = defaultdict(list)

            for score in beatmap_scores:
                mods_dict[score.mods].append(score)

            for mods, scores_list in mods_dict.items():
                # Sort scores by total score
                scores_list.sort(key=lambda x: x.total_score, reverse=True)

                # Get best score with mods
                mods_best_score = scores_list.pop(0)

                # Update other scores to submitted status
                for score in scores_list:
                    best_score_ids = (mods_best_score.id, best_score.id)

                    if score.id in best_score_ids:
                        continue

                    scores.update(score.id, {'status_score': 2}, session=session)

                if mods == best_score.mods:
                    # Don't update the best score
                    continue

                # Update best mod-score
                scores.update(mods_best_score.id, {'status_score': 4}, session=session)

    app.session.logger.info(f'[users] -> Done.')

def recalculate_statuses_all(exclude_pp=False) -> None:
    """Recalculate the pp and score statuses of all users"""
    app.session.logger.info('[users] -> Recalculating statuses of all users...')

    with app.session.database.managed_session() as session:
        users_list = users.fetch_all(session=session)
        users_list.sort(key=lambda x: x.id)

        for user in users_list:
            recalculate_score_status(user.id, 0)
            recalculate_score_status(user.id, 1)
            recalculate_score_status(user.id, 2)
            recalculate_score_status(user.id, 3)

            if exclude_pp:
                continue

            recalculate_pp_status(user.id, 0)
            recalculate_pp_status(user.id, 1)
            recalculate_pp_status(user.id, 2)
            recalculate_pp_status(user.id, 3)

    app.session.logger.info('[users] -> Done.')

def recalculate_rx_scores() -> None:
    with app.session.database.managed_session() as session:
        user_scores = session.query(DBScore) \
            .filter(or_(
                DBScore.mods.op('&')(128) != 0,
                DBScore.mods.op('&')(8192) != 0
            )) \
            .order_by(DBScore.status_pp.desc()) \
            .all()
        
        app.session.logger.info(
            f'[users] -> Recalculating {len(user_scores)} rx/ap scores...'
        )
        
        for score in user_scores:
            scores.update(
                score.id,
                {'total_score': calculate_rx_score(score, score.beatmap)},
                session=session
            )

    app.session.logger.info('[users] -> Done.')

def read_csv_score_row(row: list) -> dict | None:
    if len(row) != 22:
        app.session.logger.warning(f'[users] -> Invalid row in CSV: {row}')
        return None

    return {
        'id': int(row[0]) if row[0].isdigit() else None,
        'beatmap_hash': row[1],
        'user_id': int(row[2]),
        'total_score': int(row[3]),
        'max_combo': int(row[4]),
        'mode': int(row[5]),
        'hit300': int(row[6]),
        'hit100': int(row[7]),
        'hit50': int(row[8]),
        'hit_miss': int(row[9]),
        'hit_geki': int(row[10]),
        'hit_katu': int(row[11]),
        'mods': int(row[12]),
        'grade': row[13],
        'perfect': bool(int(row[14])),
        'passed': bool(int(row[15])),
        'ranked': bool(int(row[16])),
        'submit_hash': row[17],
        'submitted_at': datetime.strptime(row[18], '%m/%d/%Y %H:%M'),
        'version': int(row[19]),
        'username': row[20],
        'replay_data_b64': row[21]
    }

def calculate_accuracy(score: DBScore) -> float:
    if score.total_objects == 0:
        return 0.0

    if score.mode == GameMode.Osu:
        return (
            ((score.n300 * 300.0) + (score.n100 * 100.0) + (score.n50 * 50.0))
            / (score.total_objects * 300.0)
        )

    elif score.mode == GameMode.Taiko:
        return (
            ((score.n100 * 0.5) + score.n300)
            / score.total_objects
        )

    elif score.mode == GameMode.CatchTheBeat:
        return (
            (score.n300 + score.n100 + score.n50)
            / score.total_objects
        )

    elif score.mode == GameMode.OsuMania:
        return (
            (
              (score.n50 * 50.0) +
              (score.n100 * 100.0) +
              (score.nKatu * 200.0) +
              ((score.n300 + score.nGeki) * 300.0)
            )
            / (score.total_objects * 300.0)
        )

    return 0.0

def upload_replay(score_data: dict, session: Session) -> None:
    if not score_data['replay_checksum']:
        app.session.logger.warning(f'[scores] -> Score with id {score_data["id"]} has no replay checksum.')
        return

    if not (score := scores.fetch_by_replay_checksum(score_data['replay_checksum'], session=session)):
        app.session.logger.warning(f'[scores] -> Score with checksum {score_data["replay_checksum"]} not found.')
        return

    app.session.storage.upload_replay(score.id, score_data['replay_data'])
    app.session.logger.info(f'[scores] -> Uploaded replay for score "{score.id}"')

def oldsu_score_migration(csv_filename: str) -> None:
    app.session.logger.info(f'[scores] -> Migrating oldsu scores from {csv_filename}...')
    
    with open(csv_filename, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        user_ids = set()
        next(reader)
        
        with app.session.database.managed_session() as session:
            for row in reader:
                score_data = read_csv_score_row(row)

                if not score_data:
                    continue
                
                try:
                    score_data['replay_data'] = base64.b64decode(score_data['replay_data_b64'])
                except Exception:
                    app.session.logger.warning(f'[scores] -> Failed to decode replay data for score: {score_data["id"]}')
                    score_data['replay_data'] = None

                score_data['replay_checksum'] = (
                    hashlib.md5(score_data['replay_data']).hexdigest()
                    if score_data['replay_data'] else None
                )

                upload_replay(score_data, session)
                user_ids.add(score_data['user_id'])

    app.session.logger.info(f'[scores] -> Migrated scores.')
    app.session.logger.info(f'[scores] -> Recalculating statuses for migrated scores...')

    for user in user_ids:
        recalculate_pp_status(user, 0)
        recalculate_pp_status(user, 1)
        recalculate_score_status(user, 0)
        recalculate_score_status(user, 1)
            
    app.session.logger.info('[scores] -> Oldsu score migration complete.')
