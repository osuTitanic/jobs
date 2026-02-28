
from app.common.database import DBBeatmap
from app import session

def i_need_to_backfill_a_shit_ton_of_data_and_it_makes_me_go_insane():
    with session.database.managed_session() as database:
        affected_maps = database.query(DBBeatmap) \
            .filter(DBBeatmap.server == 0) \
            .filter(DBBeatmap.count_normal == 0, DBBeatmap.count_slider == 0, DBBeatmap.count_spinner == 0)
            
        for beatmap in affected_maps:
            ossapi_map = session.ossapi.beatmap(beatmap.id)
            assert ossapi_map is not None, "this is fine"
            
            database.query(DBBeatmap) \
                .filter(DBBeatmap.id == beatmap.id) \
                .update({
                    DBBeatmap.count_normal: ossapi_map.count_normal,
                    DBBeatmap.count_slider: ossapi_map.count_slider,
                    DBBeatmap.count_spinner: ossapi_map.count_spinner
                })
            database.commit()

        affected_maps = database.query(DBBeatmap) \
            .filter(DBBeatmap.server == 0) \
            .filter(DBBeatmap.drain_length.in_([0, 1]))
            
        for beatmap in affected_maps:
            ossapi_map = session.ossapi.beatmap(beatmap.id)
            assert ossapi_map is not None, "this is fine"
            
            database.query(DBBeatmap) \
                .filter(DBBeatmap.id == beatmap.id) \
                .update({
                    DBBeatmap.drain_length: ossapi_map.hit_length
                })
            database.commit()
