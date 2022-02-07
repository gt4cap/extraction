# findImageCandidate - find oldest ingested image, return S3 key
#
import json

import psycopg2

from . import updateImageStatus as uis

def findImageCandidate(cardtype):
    # Spatial database details
    with open('db_config.json', 'r') as f:
        dbconfig = json.load(f)
    dbconfig = dbconfig['database']

    connString = f"host={dbconfig['connection']['host']} dbname={dbconfig['connection']['dbname']}\
  user={dbconfig['connection']['dbuser']} port={dbconfig['connection']['port']}\
  password={dbconfig['connection']['dbpasswd']}"

    inconn = psycopg2.connect(connString)
    if not inconn:
        print("No in connection established")
        return(None)

    # Get the first image record that is not yet processed
    imagesql = f"SELECT id, reference, obstime from {dbconfig['tables']['catalog_table']}, {dbconfig['tables']['aoi_table']}\
    where footprint && wkb_geometry and {dbconfig['args']['aoi_field']} = '{dbconfig['args']['name']}'\
    and obstime between '{dbconfig['args']['startdate']}' and '{dbconfig['args']['enddate']}'\
    and status ='ingested'\
    and card='{cardtype}' order by obstime asc limit 1"
    #print(imagesql)
    with inconn:
        with inconn.cursor() as trans_cur:
            trans_cur.execute(imagesql)
            if trans_cur.rowcount != 1:
                print("No images with status 'ingested' found")
                return(None)
            else:
                result = trans_cur.fetchone()
                oid = result[0]
                reference = result[1]
                obstime = result[2]
                # Fails if this record is changed in the meantime
                if (uis.updateImageStatus(oid, 'inprogress', 'ingested')):
                    print(f"{reference} found for processing")
                    return oid, reference, obstime
                else:
                    return None
