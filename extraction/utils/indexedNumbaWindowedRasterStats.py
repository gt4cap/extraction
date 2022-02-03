import io

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

import json

import rasterio
from rasterio.windows import Window

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

@numba.njit()
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

@numba.njit()
def getMeans(chunk, nodata, checked=False):
    # If this is from a subsequent band, zeros are already eliminated
    if checked:
        return len(chunk), np.mean(chunk)

    # else, first check whether this selection contains nodata
    count = len(chunk)
    if count:
        dmax = np.max(chunk)
        dmin = np.min(chunk)

        if dmax != nodata:
            if dmin != nodata:
                return count, np.mean(chunk)
    return None

def indexRasterStats(oid, reference, bands, card):
    # the multiband image is expected as a VRT with single pixel spacing and projection
    # first read only metadata so that we can set up windowed reads
    with rasterio.open(f"data/{reference}.vrt") as src:
        profile = src.profile
        imgcrs = src.crs.to_epsg()

    imgulx = profile['transform'][2]
    imguly = profile['transform'][5]
    # dx is needed to select the correct rasterized parcels and band resolutions
    dx = int(np.round(profile['transform'][0]))

    imgwidth = profile['width']
    imgheight = profile['height']
    print(imgulx, imguly)
    print(f"full image dimension {imgwidth}*{imgheight} requires {4*imgwidth*imgheight/(1024*1024)} MB")    

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
        return "No in db"

    incurs = inconn.cursor()

    catalog_table = f"{dbconfig['tables']['catalog_table']}"
    parcel_vector_table = f"{dbconfig['tables']['parcel_table']}"
    parcel_raster_table = f"{dbconfig['tables']['parcel_table']}_{imgcrs}_{dx}_rast"

    # Select the extent of the parcel selection in this image footprint
    extSql = f"""with ext as (select st_extent(st_transform(wkb_geometry, %s)) bbox from aois 
    where name = %s) select st_xmin(bbox) ulx, st_ymax(bbox) uly, st_xmax(bbox) lrx, st_ymin(bbox) lry from ext
    """

    try:
        incurs.execute(extSql, (imgcrs, dbconfig['args']['name']))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        inconn.close()
        return "No extent"

    pclext = incurs.fetchone()
    #print(pclext) 
    wulx = pclext[0]
    if wulx < imgulx:
        wulx = imgulx
    
    wuly = pclext[1]
    if wuly > imguly:
        wuly = imguly
        
    wlrx = pclext[2]
    
    if wlrx > imgulx + imgwidth*dx:
        wlrx = imgulx + imgwidth*dx
        
    wlry = pclext[3]
    
    if wlry < imguly - imgheight*dx:
        wlry = imguly - imgheight*dx
    
    #print(wulx, wuly, wlrx, wlry)
    
    w0x = int(np.floor((wulx-imgulx)/dx))
    w0y = int(np.floor((imguly-wuly)/dx)) 
    wdx = int(np.ceil((wlrx-wulx)/dx))
    wdy = int(np.ceil((wuly-wlry)/dx))

    print(w0x, w0y, wdx, wdy)

    # Now we're ready to do a windowed read    
    with rasterio.open(f"data/{reference}.vrt") as src:    
        try:
            data = src.read(window=Window(w0x, w0y, wdx, wdy))
        except (Exception, rasterio.errors.RasterioIOError) as e:
            # This likely occurs only if there is a memory error
            print(e)
            inconn.close()
            return "Rio error"

    dims = data.shape
    print(f"partial image dimension {dims[1]}*{dims[2]} requires {4*dims[1]*dims[2]/(1024*1024)} MB ({100.0*dims[1]*dims[2]/(imgheight*imgwidth)})")    
    
    ulx = wulx
    uly = wuly
    
    # Close cursor to allow for named cursor
    incurs.close()

    # we need a named cursor to be able to use fetchmany
    incurs = inconn.cursor(name='fetch_raster_parcels', cursor_factory=psycopg2.extras.DictCursor)

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
        return "Parcel SQL"

    totalrows = 0
    
    outconn = psycopg2.connect(connString)
    if not outconn:
        print("No out connection established")
        return "No out db"

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
    if totalrows == 0:
        return "No parcels"
    else:
        return "extracted"

def indexRasterMeans(v):
    # the single band image is expected as a VRT with single pixel spacing and projection
    # first read only metadata so that we can set up windowed reads
    with rasterio.open(v) as src:
        profile = src.profile
        imgcrs = src.crs.to_epsg()

    imgulx = profile['transform'][2]
    imguly = profile['transform'][5]
    # dx is needed to select the correct rasterized parcels and band resolutions
    dx = int(np.round(profile['transform'][0]))

    imgwidth = profile['width']
    imgheight = profile['height']
    print(imgulx, imguly)
    print(f"full image dimension {imgwidth}*{imgheight} requires {4*imgwidth*imgheight/(1024*1024)} MB")    

    # 2. database configuration parsing from json
    with open('db_config_vrts.json', 'r') as f:
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
        return "No in db"

    incurs = inconn.cursor()

    parcel_vector_table = f"{dbconfig['tables']['parcel_table']}"
    parcel_raster_table = f"{dbconfig['tables']['parcel_table']}_{imgcrs}_{dx}_rast"

    # Select the extent of the parcel selection in this image footprint and projection
    extSql = f"""with ext as (select st_transform(st_setsrid(st_extent(wkb_geometry), 
    (select distinct st_srid(wkb_geometry) from {parcel_vector_table})), {imgcrs}) bbox from {parcel_vector_table}) 
    select st_xmin(bbox) ulx, st_ymax(bbox) uly, st_xmax(bbox) lrx, st_ymin(bbox) lry from ext;"""
    
    try:
        incurs.execute(extSql)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        inconn.close()
        return "No extent"

    pclext = incurs.fetchone()
    #print(pclext) 
    wulx = pclext[0]
    if wulx < imgulx:
        wulx = imgulx
    
    wuly = pclext[1]
    if wuly > imguly:
        wuly = imguly
        
    wlrx = pclext[2]
    
    if wlrx > imgulx + imgwidth*dx:
        wlrx = imgulx + imgwidth*dx
        
    wlry = pclext[3]
    
    if wlry < imguly - imgheight*dx:
        wlry = imguly - imgheight*dx
    
    #print(wulx, wuly, wlrx, wlry)
    
    w0x = int(np.floor((wulx-imgulx)/dx))
    w0y = int(np.floor((imguly-wuly)/dx)) 
    wdx = int(np.ceil((wlrx-wulx)/dx))
    wdy = int(np.ceil((wuly-wlry)/dx))

    print(w0x, w0y, wdx, wdy)

    # Now we're ready to do a windowed read    
    with rasterio.open(v) as src:    
        try:
            data = src.read(window=Window(w0x, w0y, wdx, wdy))
        except (Exception, rasterio.errors.RasterioIOError) as e:
            # This likely occurs only if there is a memory error
            print(e)
            inconn.close()
            return "Rio error"

    dims = data.shape
    print(f"partial image dimension {dims[1]}*{dims[2]} requires {4*dims[1]*dims[2]/(1024*1024)} MB ({100.0*dims[1]*dims[2]/(imgheight*imgwidth)})")    
    
    ulx = wulx
    uly = wuly
    
    # Close cursor to allow for named cursor
    incurs.close()

    # we need a named cursor to be able to use fetchmany
    incurs = inconn.cursor(name='fetch_raster_parcels', cursor_factory=psycopg2.extras.DictCursor)

    # Select the parcels in this image footprint, in the correct rasterized format
    pidSql = f"""select pid, st_asbinary(rast) from {parcel_raster_table}"""

    try:
        incurs.execute(pidSql)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        inconn.close()
        return "Parcel SQL"

    totalrows = 0
    
    outconn = psycopg2.connect(connString)
    if not outconn:
        print("No out connection established")
        return "No out db"

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
                b0 = getMeans(data[0, np.uint16(rows), np.uint16(cols)], 0.0)
                if b0:
                    tseries.append((r[0], *b0))

        # Prepare as pandas DataFrame and copy into database
        df = pd.DataFrame(tseries, columns = ['pid', 'count', 'mean'])
        df['rtfid'] = v.split('/')[-1].replace('.vrt', '')

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
    if totalrows == 0:
        return "No parcels"
    else:
        return "Extracted"
