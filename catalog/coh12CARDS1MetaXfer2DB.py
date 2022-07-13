#!/usr/bin/env python
# coding: utf-8

# # Get And Transfer CARD Metadata from vrt files on CREODIAS storage
#

import os
import sys
import glob
import json
import psycopg2
from osgeo import gdal, osr
from datetime import datetime


def main(aoi_name=None):
    # docker run -it --rm -v`pwd`:/usr/src/app -v /mnt:/mnt glemoine62/
    #   dias_numba_py python coh12CARDS1MetaXfer2DB.py
    vrts = glob.glob('/mnt/smbpub/jrc/Sentinel-1/S1A/COH12/VV/*.vrt')
    print('Number of vrt files to process: ', len(vrts))

    with open('cat_config.json', 'r') as f:
        dbconfig = json.load(f)['database']
    dbconn = dbconfig['connection']

    conn_str = f"host={dbconn['host']} dbname={dbconn['dbname']}\
        user={dbconn['dbuser']} port={dbconn['port']}\
        password={dbconn['dbpasswd']}"

    def check_exists(ref):
        sql = f"""SELECT id
            FROM {dbconfig['tables']['catalog_table']}
            WHERE reference = '{ref}' """
        cursor.execute(sql)
        return cursor.fetchone() is not None

    new_entries = 0
    try:
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()
        print("Conected to database")

        progress_bar(0, len(vrts), '')
        for i, vrt in enumerate(vrts):
            ref = os.path.splitext(os.path.basename(vrt))[0]
            try:
                if not check_exists(ref):
                    cursor.execute(insert_card_sql(vrt))
                    connection.commit()
                    progress_bar(i + 1, len(vrts),
                                 f"- reference '{ref}' added.",
                                 color=bcolors.OKCYAN)
                    new_entries += 1
                else:
                    progress_bar(i + 1, len(vrts),
                                 f"- reference '{ref}' exist.")
            except Exception:
                progress_bar(i + 1, len(vrts), f"- reference '{ref}' ERROR.",
                             color=bcolors.FAIL)
                cursor.close()
                connection.close()
                connection = psycopg2.connect(conn_str)
                cursor = connection.cursor()

    except (Exception, psycopg2.Error) as err:
        print("Error while fetching data from PostgreSQL", err)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print()
            print("PostgreSQL connection is closed")
            print(f"{new_entries} new entries added to dias_catalogue table.")
    if aoi_name:
        get_card_stats(aoi_name)


def insert_card_sql(vrt):
    # Each record get the _status_ 'ingested' by default.
    reference = os.path.splitext(os.path.basename(vrt))[0]
    obstime = reference.split('_')[5]

    def GetExtent(ds):
        """ Return list of corner coordinates from a gdal Dataset """
        xmin, xpixel, _, ymax, _, ypixel = ds.GetGeoTransform()
        width, height = ds.RasterXSize, ds.RasterYSize
        xmax = xmin + width * xpixel
        ymin = ymax + height * ypixel

        return (xmin, ymax), (xmax, ymax), (xmax, ymin), (xmin, ymin)

    def ReprojectCoords(coords, src_srs, tgt_srs):
        """ Reproject a list of x,y coordinates. """
        trans_coords = []
        transform = osr.CoordinateTransformation(src_srs, tgt_srs)
        for x, y in coords:
            x, y, z = transform.TransformPoint(x, y)
            trans_coords.append([x, y])
        return trans_coords

    ds = gdal.Open(vrt)

    ext = GetExtent(ds)
    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())
    tgt_srs = osr.SpatialReference()
    tgt_srs.ImportFromEPSG(4326)
    # tgt_srs = src_srs.CloneGeogCS()
    geo_ext = ReprojectCoords(ext, src_srs, tgt_srs)
    # geo_ext = [[c[1], c[0]] for c in geo_ext]
    geo_ext.append(geo_ext[0])
    geo_ext_text = str(geo_ext).replace(', ', ' ').replace(
        '] [', ', ').replace(']', '').replace('[', '')

    sql = f"""
    INSERT INTO dias_catalogue (obstime, reference, sensor, card, footprint)
    VALUES (
        '{obstime}'::timestamp,
        '{reference}',
        '1A',
        'c1',
        ST_GeomFromText('POLYGON(({geo_ext_text}))', 4326)
    ) On conflict (reference) Do nothing;
    """
    return sql


def get_card_stats(aoi_name):
    with open('cat_config.json', 'r') as f:
        dbconfig = json.load(f)['database']
    dbconn = dbconfig['connection']

    conn_str = f"host={dbconn['host']} dbname={dbconn['dbname']}\
        user={dbconn['dbuser']} port={dbconn['port']}\
        password={dbconn['dbpasswd']}"

    # Get CARD types statistics that are available for this area of interest
    getMetadataSql = f"SELECT card, sensor, count(*), min(obstime),\
        max(obstime) FROM {dbconfig['tables']['catalog_table']},\
        {dbconfig['tables']['aoi_table']}\
        WHERE st_intersects(footprint, wkb_geometry)\
        And {dbconfig['args']['aoi_field']} = '{aoi_name}'\
        GROUP By card, sensor order by card, sensor;"

    try:
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()
        print("Conected to database")
        cursor.execute(getMetadataSql)
        # Get the columns names for the rows
        colnames = [desc[0] for desc in cursor.description]
        print(colnames)

        for rows in cursor:
            print(rows[0:3], datetime.strftime(rows[3], '%Y-%m-%d %H:%M:%S'),
                  datetime.strftime(rows[4], '%Y-%m-%d %H:%M:%S'))
    except (Exception, psycopg2.Error) as err:
        print("[Err] Error while fetching data from PostgreSQL", err)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def progress_bar(progress, total, text='', length=20,
                 color=bcolors.WARNING):
    percent = int(100 * (progress / total))
    bar = '░' * int(percent / (100 / length)) + '▭' * \
        (length - int(percent / (100 / length)))
    print(f"\r{color}|{bar}| {percent}% " + text + bcolors.ENDC, end="\r")
    if progress == total:
        print(f"\r{bcolors.OKGREEN}|{bar}| {percent}% - " +
              "- - Process Completed. -" + bcolors.ENDC, end="\r")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        aoi_name = sys.argv[1]
        main(aoi_name)
    else:
        main()
