import io

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

import json

import rasterio

from . import wkbReader

import numba

def truncate(r, c, rmin, rmax, cmin, cmax):
    nor = np.where((r < rmin) | (r > rmax))
    if nor:
        r = np.delete(r, nor)
        c = np.delete(c, nor)

    noc = np.where((c < cmin) | (c > cmax))

    if noc:
        r = np.delete(r, noc)
        c = np.delete(c, noc)
    return r, c

#@numba.jit(nopython=True)
def getIndices(pulx, puly, prast, ulx, uly, dx, dims):
    ind = prast[0]
    drow = (uly - puly)/dx
    dcol = (pulx - ulx)/dx
    r,c = np.where(ind == 1)

    return truncate(int(np.floor(drow)) - r, int(np.floor(dcol)) + c, 0, dims[1]-1, 0, dims[2]-1)

@numba.jit(nopython=True)
def getStats(chunk, nodata, checked=False):
    # If this is from a subsequent band, zeros are already eliminated
    if checked:
        return len(chunk), np.mean(chunk), np.std(chunk), np.min(chunk), np.max(chunk), np.percentile(chunk, 25), np.percentile(chunk, 50), np.percentile(chunk, 75)

    # else, first check whether this selection contains nodata
    count = len(chunk)
    if count:
        dmax = np.max(chunk)
        dmin = np.min(chunk)

        if dmax != nodata:
            if dmin != nodata:
                return count, np.mean(chunk), np.std(chunk), dmin, dmax, np.percentile(chunk, 25), np.percentile(chunk, 50), np.percentile(chunk, 75)
    return None

def indexRasterStats(oid, reference, bands, card):
    # the multiband image is expected as a VRT with single pixel spacing and projection
    with rasterio.open(f"data/{reference}.vrt") as src:
        profile = src.profile
        data = src.read()
        imgcrs = src.crs.to_epsg()

    dims = data.shape
    ulx = profile['transform'][2]
    uly = profile['transform'][5]
    # dx is needed to select the correct rasterized parcels and band resolutions
    dx = int(np.round(profile['transform'][0]))

    # 2. database configuration parsing from json
    with open('db_config_sigs.json', 'r') as f:
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

    catalog_table = f"{dbconfig['tables']['catalog_table']}"
    parcel_vector_table = f"{dbconfig['tables']['parcel_table']}"
    parcel_raster_table = f"{dbconfig['tables']['parcel_table']}_{imgcrs}_{dx}_rast"

    # Select the parcels in this image footprint, in the correct rasterized format
    pidSql = f"""with crs as (select srid from geometry_columns where f_table_name = %s and f_table_schema = %s) 
    select pid, st_asbinary(rast) from {parcel_raster_table}, {parcel_vector_table} 
    where pid = ogc_fid and wkb_geometry && st_transform((select footprint from {catalog_table} where id = %s),
    (select srid from crs))"""

    try:
        incurs.execute(pidSql, (parcel_vector_table.split('.')[1], parcel_vector_table.split('.')[0], oid))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        inconn.close()
        return False

    totalrows = 0
    
    outconn = psycopg2.connect(connString)
    if not outconn:
        print("No out connection established")
        return False

    # band renaming for S1
    if card == 'bs':
        bands = [b + 'b' for b in bands]
    elif card == 'c6':
        bands = [b + 'c' for b in bands]

    while True:
        rowset = incurs.fetchmany(size=10000)
        if not rowset:
            break;

        tseries = []
        for r in rowset:
            rast = r[1]

            pulx, puly, prast = wkbReader.wkbImage(rast)

            rows, cols = getIndices(pulx, puly, prast, ulx, uly, dx, dims)

            if len(rows)>0 or len(cols)>0:
                b0 = getStats(data[0, np.uint16(rows), np.uint16(cols)], 0.0)
                if b0:
                    tseries.append((r[0], bands[0], *b0))
                    for b in range(1, len(bands)): 
                        bN= getStats(data[b, np.uint16(rows), np.uint16(cols)], 0.0, True)
                        tseries.append((r[0], bands[b], *bN))

        # Prepare as pandas DataFrame and copy into database
        df = pd.DataFrame(tseries, columns = ['pid', 'band', 'count', 'mean', 'std', 'min', 'max', 'p25', 'p50', 'p75'])
        df['obsid'] = oid
        if len(df) > 0:
            df.dropna(inplace=True)
            #print(df)
            totalrows += len(df)
            if len(df.values) > 0:
                df_columns = list(df)
                #print(df_columns)
                s_buf = io.StringIO()
                df.to_csv(s_buf, header=False, index=False, sep=',')
                s_buf.seek(0)
                outcurs = outconn.cursor()
                try:
                    outcurs.copy_from(s_buf, dbconfig['tables']['results_table'], columns = tuple(df_columns), sep = ',')
                    outconn.commit()
                except psycopg2.IntegrityError as e:
                    print("IntegrityError")
                except psycopg2.DatabaseError as e:
                    print(e)
                    print("DatabaseError")
                except Error as e:
                    print(e)
                finally:
                    outcurs.close()

    incurs.close()
    inconn.close()
    outconn.close()
    print(f"{totalrows} processed")
    return totalrows

