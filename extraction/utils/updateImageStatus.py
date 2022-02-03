# updateImageStatus - update image status 
#
import json

import psycopg2

def updateImageStatus(oid, new_status, current_status):
    # Spatial database details
    with open('db_config_sigs.json', 'r') as f:
        dbconfig = json.load(f)
    dbconfig = dbconfig['database']

    connString = "host={} dbname={} user={} port={} password={}".format(\
        dbconfig['connection']['host'], dbconfig['connection']['dbname'],\
        dbconfig['connection']['dbuser'], dbconfig['connection']['port'],\
        dbconfig['connection']['dbpasswd'])

    conn = psycopg2.connect(connString)
    if not conn:
        print("No connection established")
        return(None)

    updateSql = f"update {dbconfig['tables']['catalog_table']} set status='{new_status}' where id = {oid} and status = '{current_status}'"""
    with conn.cursor() as update_cur:
        update_cur.execute(updateSql)
        conn.commit()
        if update_cur.rowcount == 1:
            print(f"Record for {oid} updated to {new_status} from {current_status}")
            conn.close()
            return True

    conn.close()
    return False
