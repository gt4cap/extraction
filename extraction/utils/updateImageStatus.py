# updateImageStatus - update image status
#
import json
import psycopg2


def updateImageStatus(oid, new_status, current_status):
    # Spatial database details
    with open('db_config.json', 'r') as f:
        dbconfig = json.load(f)['database']
    dbconn = dbconfig['connection']

    conn_str = f"host={dbconn['host']} dbname={dbconn['dbname']}\
        user={dbconn['dbuser']} port={dbconn['port']}\
        password={dbconn['dbpasswd']}"

    conn = psycopg2.connect(conn_str)
    if not conn:
        print("No connection established")
        return(None)

    updateSql = f"""UPDATE {dbconfig['tables']['catalog_table']}
        SET status='{new_status}'
        WHERE id = {oid} And status = '{current_status}'"""
    with conn.cursor() as update_cur:
        update_cur.execute(updateSql)
        conn.commit()
        if update_cur.rowcount == 1:
            print(f"Record for {oid} updated to {new_status} from {current_status}")
            conn.close()
            return True

    conn.close()
    return False
