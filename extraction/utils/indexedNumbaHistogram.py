import io
import sys

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

import json

import rasterio

from . import wkbReader

import numba

@numba.njit()
def chop(arr, amin, amax):
    mask = np.zeros(arr.shape[0], dtype=np.int64) == 0
    mask[np.where(np.logical_or(arr < amin, arr > amax))[0]] = False
    return mask

@numba.njit()
def truncate(r, c, rmin, rmax, cmin, cmax):
    rmask = chop(r, rmin, rmax)
    r = r[rmask]
    c = c[rmask]
    cmask = chop(c, cmin, cmax)
    return r[cmask], c[cmask]

#@numba.njit()
def getIndices(pulx, puly, prast, ulx, uly, dx, dims):
    ind = prast[0]
    drow = (uly - puly)/dx
    dcol = (pulx - ulx)/dx
    r,c = np.where(ind == 1)

    return truncate(int(np.floor(drow)) - r, int(np.floor(dcol)) + c, 0, dims[1]-1, 0, dims[2]-1)

#@numba.njit()
def getHistogram(chunk):
    return np.unique(chunk, return_counts=True)

def indexRasterHistogram(refs):
    # the multiband image is expected as a VRT with single pixel spacing and projection
    with rasterio.open(f"data/{refs[1]}.vrt") as src:
        profile = src.profile
        if profile['count']!= 1:
            print(f"Number of bands in VRT must be 1")
            return False
        data = src.read()
        imgcrs = src.crs.to_epsg()

    dims = data.shape
    ulx = profile['transform'][2]
    uly = profile['transform'][5]
    dx = profile['transform'][0]
    nodata = profile['nodata']

    # 2. database configuration parsing from json
    with open('db_config.json', 'r') as f:
        dbconfig = json.load(f)
    dbconfig = dbconfig['database']

    # Input data base is postgis
    connString = "host={} dbname={} user={} port={} password={}".format(\
        dbconfig['connection']['host'], dbconfig['connection']['dbname'],\
        dbconfig['connection']['dbuser'], dbconfig['connection']['port'],\
        dbconfig['connection']['dbpasswd'])

    inconn = psycopg2.connect(connString)
    if not inconn:
        print("No in connection established")
        return False

    # we need a named cursor to be able to use fetchmany
    incurs = inconn.cursor(name='fetch_raster_parcels', cursor_factory=psycopg2.extras.DictCursor)

    parcel_vector_table = f"{dbconfig['tables']['parcel_table']}"
    parcel_raster_table = f"{dbconfig['tables']['parcel_table']}_{imgcrs}_{int(np.round(dx))}_rast"

    # Select the parcels in this image footprint, in the correct rasterized format
    pidSql = f"with crs as (select srid from geometry_columns where f_table_name = %s and f_table_schema = %s) \
    select pid, st_asbinary(rast) from {parcel_raster_table}, {parcel_vector_table} \
    where pid = ogc_fid and wkb_geometry && \
    st_transform(st_makeenvelope(%s, %s, %s, %s, %s), (select srid from crs))"

    try:
        incurs.execute(pidSql, (parcel_vector_table.split('.')[1], parcel_vector_table.split('.')[0], ulx, uly - dims[2]*dx, ulx + dims[1]*dx, uly, imgcrs))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        inconn.close()
        return False

    totalrows = 0

    outconn = psycopg2.connect(connString)
    if not outconn:
        print("No out connection established")
        return False


    while True:
        outcurs = outconn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        rowset = incurs.fetchmany(size=1000)
        if not rowset:
            break;

        records = []
        for r in rowset:
            rast = r[1]

            pulx, puly, prast = wkbReader.wkbImage(rast)

            rows, cols = getIndices(pulx, puly, prast, ulx, uly, dx, dims)
            
            if len(rows)>0 or len(cols)>0:
                record = {}
                ref = refs[1]
                record['pid'] = int(r[0])
                record['obsid'] = int(refs[0])
                histvals, histcounts = getHistogram(data[0, np.uint16(rows), np.uint16(cols)])
                if not ((len(histvals) == 1) and (histvals[0] == 0)):
                    dc = dict(zip(histvals, histcounts))
                    # JSON is very picky about numeric type!
                    dcst = {str(k):int(v) for k, v in dc.items()}

                    if dc:
                        record['hist'] = json.dumps(dcst)
                        records.append(record)
                    else:
                        print("Empty dict")

        insert_stmt = f"insert into {dbconfig['tables']['hists_table']}  (pid, obsid, hist) values (%(pid)s, %(obsid)s, %(hist)s);"

        if len(records) > 0:
            try:
                psycopg2.extras.execute_batch(outcurs, insert_stmt, records)
                outconn.commit()
                totalrows += len(records)
            except psycopg2.IntegrityError as e:
                print("insert statement {} contains duplicate index".format(insert_stmt))
            except:
                #print(records)
                print(sys.exc_info()[0])
                sys.exit(1)

            finally:
                outcurs.close()

    incurs.close()
    inconn.close()
    outconn.close()
    print(f"{totalrows} processed")
    return totalrows
