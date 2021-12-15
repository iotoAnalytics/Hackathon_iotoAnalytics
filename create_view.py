from database import CursorFromConnectionFromPool
from psycopg2 import sql

VIEW_NAME = "us_legislators"
SELECTED_COLUMNS = ["goverlytics_id", "name_full", "name_last", "name_first", "name_middle", "name_suffix", "district", "party_id", "state", "wiki_url", "is_active"]
COMPLETED_STATES = ["AK", "AL", "AZ", "DE", "HI", "IA", "ID", "KS", "KY", "LA", "MA", "NC", "ND", "NE", "NM", "NV", "OH", "RI", "SC", "UT", "VT", "WA", "WY"]

def get_select_query():
    query = ""
    for state in COMPLETED_STATES:
        table = "us_" + state.lower() + "_legislators"
        if state == COMPLETED_STATES[-1]:
            query += " SELECT " + ", ".join(SELECTED_COLUMNS) + " FROM " + table + ";"
        else:
            query += " SELECT " + ", ".join(SELECTED_COLUMNS) + " FROM " + table + " UNION"
    return query

select_query = get_select_query()

query = sql.SQL("""
    CREATE OR REPLACE VIEW {view_name}
    AS     
    {select_query}

    ALTER TABLE {view_name}
        OWNER TO rds_ad;
""").format(
    view_name=sql.Identifier(VIEW_NAME),
    select_query=sql.SQL(select_query)
)

with CursorFromConnectionFromPool() as cursor:
    try:
        cursor.execute(query)
        cursor.connection.commit()
    except Exception as e:
        print(f"error:\n {e}")
        cursor.connection.rollback()