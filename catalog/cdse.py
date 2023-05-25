#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This file is part of next generation CbM (https://github.com/gt4cap).
# Author    : Guido Lemoine, Konstantinos Anastasakis
# Credits   : LUCHAP Team
# Copyright : 2023 European Commission, Joint Research Centre
# Version   : 1.1, adapted from scripts/creodias.py to reflect changes to CDSE
#             postgis < 3.0 compatible for footprint insert
#             database configuration from json file
# License   : 3-Clause BSD


# Query And Transfer CARD Metadata with requests and json parsing (CDSE)
#
# CDSE ingests the metadata for the generated CARD data in the
# [CDSE public catalog](http://datahub.creodias.eu)
#
# The catalog interface uses the OpenSearch API, a standard to define catalog
# queries with a set of standards query parameters, and which produces a
# JSON formatted response which we can parse into our data base.
#
#

import sys
import json
import requests
import psycopg2
from datetime import datetime
#from cbm.datas import db

def cdse(aoi, start, end, card, option):
    """
    aoi: name of the area of interest for which an entry is created in table aois
    start, end : YYYY-MM-dd formated date
    card : s2, c6. c12 or bs
    option: c6 = CARD-COH6, c12 = CARD-COH12, bs = CARD-BS, s2 = S2MSI2A or S2MSI1C
    """

    root = "http://datahub.creodias.eu"
    path = "/resto/api/collections/Sentinel{}/search.json?"

    # CREODIAS API Requests for card catalogue
    # We adapt the period to make sure we get no more than 2000 records
    # (the maximum number of return records).
    p1 = "maxRecords=2000&startDate={}&completionDate={}"
    if card == 's2':
        p2 = "&processingLevel={}&sortParam=startDate"
        sat = '2'
    else:
        p2 = "&productType={}&sortParam=startDate"
        sat = '1'
    p3 = "&sortOrder=descending&status=all&geometry={}&dataset=ESA-DATASET"
    url = f"{root}{path}{p1}{p2}{p3}"

    startDate = '{}T00:00:00Z'.format(start)
    endDate = '{}T00:00:00Z'.format(end)

    with open('db_config.json', 'r') as f:
        dbconfig = json.load(f)
        dbconfig = dbconfig['database']

    # Input data base is postgis

    try:
        connString = f"host={dbconfig['connection']['host']} dbname={dbconfig['connection']['dbname']} user={dbconfig['connection']['dbuser']} port={dbconfig['connection']['port']} password={dbconfig['connection']['dbpasswd']}"
        conn = psycopg2.connect(connString)
        cur = conn.cursor()
    except Exception as error:
        print("Can not connect to the database", error)
        sys.exit(1)

    try:
        selectSql = f"select st_astext(wkb_geometry) from aois where name = '{aoi}';"
        cur.execute(selectSql)
    except Exception as error:
        print(f"Can not find geometry for {aoi}")

    row = cur.fetchone()
    geometry = row[0]

    url = url.format(sat, startDate, endDate, option, geometry.replace(' ', '+'))

    print(url)
    r = requests.get(url)
    contentType = r.headers.get('content-type').lower()

    # If all goes well, we should get 'application/json' as contentType
    # print(contentType)

    if contentType.find('json') == -1:
        print(f"FAIL: Server does not return JSON content for metadata, {contentType}.")
        print(r.content)
        sys.exit(1)
    else:
        out = r.json()
        if 'detail' in list(out.keys()) and list(out['detail'].keys())[0].startswith('Error'):
            print("FAIL: datahub returns error")
            sys.exit(1)
        else:
            entries = out['features']
            print(f"{len(entries)} CARD entries found ...")

    # Prepare the database for record ingestion
    insertSql = """
        INSERT into dias_catalogue (obstime, reference,
            sensor, card, footprint)
        values ('{}'::timestamp, '{}', '{}', '{}', ST_SetSRID(ST_GeomFromGeoJSON($${}$$), 4326))
        """

    # The XML parsing will select relevant metadata parameters and reformats
    # these into records to insert into the __dias_catalogue__ table.
    # Note that rerunning the parsing will skip records that are already in
    # the table with an existing reference attribute (the primary key).
    # Rerunning will, thus, only add new records.

    if len(entries) > 0:
        for e in entries:
            title = e['properties']['title']
            sensor = title[1:3].strip()
            tstamp = e['properties']['startDate']
            footprint = e['geometry']

            try:
                print(insertSql.format(tstamp.replace('T', ' '),
                    title, sensor, card, footprint))
                try:
                    cur.execute(insertSql.format(tstamp.replace('T', ' '),
                        title, sensor, card, footprint))
                    conn.commit()
                except psycopg2.IntegrityError as err:
                    print("Skipping duplicate record!", err)
                    conn.rollback()

            except Exception:
                print(f"Polygon parsing issues for {title} with footprint {footprint}")

        # Get statistics on CARD types that are available for this area of interest
        getMetadataSql = f"""
            SELECT card, sensor, count(*), min(obstime), max(obstime)
            FROM dias_catalogue
            WHERE st_intersects(footprint, st_geomfromtext('{geometry}', 4326))
            GROUP by card, sensor
            ORDER by card, sensor;
        """

        cur.execute(getMetadataSql)
        # Get the columns names for the rows
        print("Sample entries:")
        colnames = [desc[0] for desc in cur.description]
        print(colnames)

        for rows in cur:
            print(rows[0:3], datetime.strftime(rows[3], '%Y-%m-%d %H:%M:%S'),
                  datetime.strftime(rows[4], '%Y-%m-%d %H:%M:%S'))

        # Each record get the _status_ 'ingested' by default.
        # Close database connection

    cur.close()
    conn.close()

if __name__ == "__main__":
    print(sys.argv)
    cdse(*sys.argv[1:])
