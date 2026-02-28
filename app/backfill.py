
from app.common.database import DBBeatmap
from app import session

def i_need_to_backfill_a_shit_ton_of_data_and_it_makes_me_go_insane():
    with session.database.managed_session() as database:
        affected_maps = database.query(DBBeatmap) \
            .filter(DBBeatmap.id < 1000000000) \
            .filter(DBBeatmap.count_normal == 0, DBBeatmap.count_slider == 0, DBBeatmap.count_spinner == 0)
        
        session.logger.info(
            f'Found {affected_maps.count()} beatmaps to backfill with missing objects'
        )
            
        for beatmap in affected_maps:
            ossapi_map = session.ossapi.beatmap(beatmap.id)
            assert ossapi_map is not None, "this is fine"
            
            database.query(DBBeatmap) \
                .filter(DBBeatmap.id == beatmap.id) \
                .update({
                    DBBeatmap.count_normal: ossapi_map.count_circles,
                    DBBeatmap.count_slider: ossapi_map.count_sliders,
                    DBBeatmap.count_spinner: ossapi_map.count_spinners
                })
            database.commit()

        affected_maps = database.query(DBBeatmap) \
            .filter(DBBeatmap.id < 1000000000) \
            .filter(DBBeatmap.drain_length.in_([0, 1]))
            
        session.logger.info(
            f'Found {affected_maps.count()} beatmaps to backfill with missing drain length'
        )

        for beatmap in affected_maps:
            ossapi_map = session.ossapi.beatmap(beatmap.id)
            assert ossapi_map is not None, "this is fine"
            
            database.query(DBBeatmap) \
                .filter(DBBeatmap.id == beatmap.id) \
                .update({
                    DBBeatmap.drain_length: ossapi_map.hit_length
                })
            database.commit()
