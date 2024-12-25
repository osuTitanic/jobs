
from app.common.cache import leaderboards, usercount as redis_usercount
from app.common.database.objects import DBBeatmap, DBReplayHistory, DBScore, DBStats
from app.common.database import beatmaps, scores, stats, users, histories
from app.common.database import usercount as db_usercount
from app.common.helpers import performance

from datetime import timedelta
from sqlalchemy import func
from typing import List

import app.session
import config

def update_website_stats() -> None:
    """Update the stats required for the website"""
    with app.session.database.managed_session() as session:
        app.session.redis.set('bancho:totalusers', users.fetch_count(session=session))
        app.session.redis.set('bancho:totalbeatmaps', beatmaps.fetch_count(session=session))
        app.session.redis.set('bancho:totalscores', scores.fetch_total_count(session=session))

def update_usercount_history() -> None:
    """Update the usercount history, displayed on the website"""
    with app.session.database.managed_session() as session:
        db_usercount.create(
            count := redis_usercount.get(),
            session=session
        )
        app.session.logger.info(
            f'[usercount] -> Created usercount entry ({count} players).'
        )

        if rows := db_usercount.delete_old(timedelta(weeks=1), session=session):
            app.session.logger.info(
                f'[usercount] -> Deleted old usercount entries ({rows} rows affected).'
            )

def recalculate_stats(user_id: int, mode: int) -> None:
    with app.session.database.managed_session() as session:
        if not (player := users.fetch_by_id(user_id, session=session)):
            app.session.logger.warning(f'[stats] -> User "{user_id}" was not found.')
            return

        best_scores_by_score = scores.fetch_best_by_score(
            user_id,
            mode,
            session=session
        )

        best_scores = scores.fetch_best(
            user_id,
            mode,
            exclude_approved=(not config.APPROVED_MAP_REWARDS),
            session=session
        )

        if not (best_scores or best_scores_by_score):
            app.session.logger.warning(f'[stats] -> No scores found for user "{user_id}" in mode "{mode}".')
            return

        rx_scores = [score for score in best_scores if (score.mods & 128) != 0]
        ap_scores = [score for score in best_scores if (score.mods & 8192) != 0]
        vn_scores = [score for score in best_scores if (score.mods & 128) == 0 and (score.mods & 8192) == 0]

        session.query(DBStats) \
            .filter(DBStats.user_id == user_id) \
            .filter(DBStats.mode == mode) \
            .update({
                "acc": calculate_weighted_acc(best_scores),
                "pp": calculate_weighted_pp(best_scores),
                "pp_vn": calculate_weighted_pp(vn_scores),
                "pp_rx": calculate_weighted_pp(rx_scores),
                "pp_ap": calculate_weighted_pp(ap_scores),
                "rscore": sum(score.total_score for score in best_scores_by_score)
            })
        session.commit()

        user_stats = stats.fetch_by_mode(
            user_id,
            mode,
            session=session
        )

        leaderboards.update(
            user_stats,
            player.country.lower()
        )

        user_stats.rank = leaderboards.global_rank(
            user_stats.user_id,
            user_stats.mode
        )

        histories.update_rank(
            user_stats,
            player.country
        )

        grades = scores.fetch_grades(
            user_stats.user_id,
            user_stats.mode,
            session=session
        )

        stats.update(
            user_stats.user_id,
            user_stats.mode,
            {
                f'{grade.lower()}_count': count
                for grade, count in grades.items()
            },
            session=session
        )

        app.session.logger.info(
            f'[stats] -> Recalculated stats for user "{user_id}" in mode "{mode}".'
        )

        del best_scores, best_scores_by_score, rx_scores, ap_scores, vn_scores

def recalculate_stats_all() -> None:
    with app.session.database.managed_session() as session:
        users_entries = users.fetch_all(session=session)

        for user in users_entries:
            recalculate_stats(user.id, 0)
            recalculate_stats(user.id, 1)
            recalculate_stats(user.id, 2)
            recalculate_stats(user.id, 3)

def restore_stats(user_id: int, remove=False) -> None:
    with app.session.database.managed_session() as session:
        if remove:
            # Force-remove all stats
            stats.delete_all(user_id, session=session)

        if not (user := users.fetch_by_id(user_id, session=session)):
            app.session.logger.warning(f'[stats] -> User "{user_id}" was not found.')
            return
        
        if user.stats:
            app.session.logger.warning(f'[stats] -> User "{user_id}" already has stats.')
            return
        
        all_stats = [
            DBStats(user_id, mode)
            for mode in range(4)
        ]

        for mode in range(4):
            score_count = session.query(DBScore) \
                .filter(DBScore.user_id == user_id) \
                .filter(DBScore.mode == mode) \
                .count()

            fail_times = session.query(
                func.sum(DBScore.failtime)
            ) \
                .filter(DBScore.user_id == user_id) \
                .filter(DBScore.mode == mode) \
                .filter(DBScore.status_pp == 1) \
                .scalar()

            fail_times = (fail_times / 1000) \
                if fail_times else 0

            map_times = session.query(
                DBScore,
                func.sum(DBBeatmap.total_length)
            ) \
                .join(DBBeatmap) \
                .group_by(DBScore) \
                .filter(DBScore.user_id == 15) \
                .filter(DBScore.mode == 0) \
                .filter(DBScore.status_pp > 1) \
                .first() or [0]

            playtime = map_times[-1] + fail_times

            combo_score = session.query(DBScore) \
                .filter(DBScore.user_id == user_id) \
                .filter(DBScore.mode == mode) \
                .filter(DBScore.hidden == False) \
                .order_by(DBScore.max_combo.desc()) \
                .first()

            max_combo = (
                combo_score.max_combo
                if combo_score else 0
            )

            total_score = session.query(
                func.sum(DBScore.total_score)
            ) \
                .filter(DBScore.user_id == user_id) \
                .filter(DBScore.hidden == False) \
                .filter(DBScore.mode == mode) \
                .scalar() or 0

            user_stats: DBStats = all_stats[mode]
            user_stats.tscore = int(total_score)
            user_stats.playcount = score_count
            user_stats.max_combo = max_combo
            user_stats.playtime = playtime

            best_scores_by_score = scores.fetch_best_by_score(
                user_id,
                mode,
                session=session
            )

            best_scores = scores.fetch_best(
                user_id,
                mode,
                exclude_approved=(not config.APPROVED_MAP_REWARDS),
                session=session
            )

            rx_scores = [score for score in best_scores if (score.mods & 128) != 0]
            ap_scores = [score for score in best_scores if (score.mods & 8192) != 0]
            vn_scores = [score for score in best_scores if (score.mods & 128) == 0 and (score.mods & 8192) == 0]

            user_stats.acc = calculate_weighted_acc(best_scores)
            user_stats.pp = calculate_weighted_pp(best_scores)
            user_stats.pp_vn = calculate_weighted_pp(vn_scores)
            user_stats.pp_rx = calculate_weighted_pp(rx_scores)
            user_stats.pp_ap = calculate_weighted_pp(ap_scores)
            user_stats.rscore = sum(score.total_score for score in best_scores_by_score)

            # Update grades
            grades = scores.fetch_grades(
                user_stats.user_id,
                user_stats.mode,
                session=session
            )

            for grade, count in grades.items():
                setattr(
                    user_stats,
                    f'{grade.lower()}_count',
                    count
                )

            # Update total hits
            total_hits_formula = {
                0: DBScore.n50 + DBScore.n100 + DBScore.n300,
                1: DBScore.n50 + DBScore.n100 + DBScore.n300,
                2: DBScore.n50 + DBScore.n100 + DBScore.n300 + DBScore.nKatu,
                3: DBScore.n50 + DBScore.n100 + DBScore.n300 + DBScore.nKatu + DBScore.nGeki
            }

            user_stats.total_hits = session.query(
                func.sum(total_hits_formula[user_stats.mode])
            ) \
                .filter(DBScore.user_id == user_stats.user_id) \
                .filter(DBScore.mode == user_stats.mode) \
                .filter(DBScore.hidden == False) \
                .scalar() or 0

            # Update replay views
            user_stats.replay_views = session.query(
                func.sum(DBReplayHistory.replay_views)
            ) \
                .filter(DBReplayHistory.user_id == user_stats.user_id) \
                .filter(DBReplayHistory.mode == user_stats.mode) \
                .scalar() or 0
            
            user_stats.ppv1 = performance.calculate_weighted_ppv1(
                best_scores,
                session=session
            )

        for user_stats in all_stats:
            session.add(user_stats)
            leaderboards.update(user_stats, user.country)

        session.commit()

def calculate_weighted_pp(scores: List[DBScore]) -> float:
    if not scores:
        return 0

    weighted_pp = sum(score.pp * 0.95**index for index, score in enumerate(scores))
    bonus_pp = 416.6667 * (1 - 0.9994 ** len(scores))
    return weighted_pp + bonus_pp

def calculate_weighted_acc(scores: List[DBScore]) -> float:
    if not scores:
        return 0

    weighted_acc = sum(score.acc * 0.95**index for index, score in enumerate(scores))
    bonus_acc = 100.0 / (20 * (1 - 0.95 ** len(scores)))
    return (weighted_acc * bonus_acc) / 100
