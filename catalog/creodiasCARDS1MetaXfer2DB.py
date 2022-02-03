#!/usr/bin/env python
# coding: utf-8

# # Query And Transfer CARD Metadata with XMLHttpRequests (CREODIAS)
# 
# CREODIAS ingests the metadata for the generated CARD data in the [CREODIAS public catalog](http://finder.creodias.eu)
# 
# The catalog interface uses the OpenSearch API, a standard to define catalog queries with a set of standards query parameters, and which produces either JSON or XML formatted response which we can parse into our data base.
# 
# In this notebook, we show how to find the data and transfer the relevant metadata records and attributes to the DIAS data base. This is primarily done to make sure we can run the same scripts across different DIAS instances using the same data base table (so that portability is easy).
# 
# We apply the concepts from the __SimpleDatabaseQueries__ notebook.
#
# Revision: 2019-09-03: Switch from georss to gml geometry parsing (georss does not support MULTIPOLYGON geometry, introduced by ESA mongols in 2019.
# Revision: 2021-04-06: externalized configuration
#
import sys
import requests
from lxml import etree
import psycopg2
import json
from datetime import datetime

with open('cat_config.json', 'r') as f:
    dbconfig = json.load(f)
dbconfig = dbconfig['database']

connString = f"host={dbconfig['connection']['host']} dbname={dbconfig['connection']['dbname']}\
  user={dbconfig['connection']['dbuser']} port={dbconfig['connection']['port']}\
  password={dbconfig['connection']['dbpasswd']}"

# print(connString)

conn = psycopg2.connect(connString)
if not conn:
	print("No in connection established")
	sys.exit(1)

curs = conn.cursor()

# Get commandline args for parameters
aoi_name = sys.argv[1]
startDate = '{}T00:00:00Z'.format(sys.argv[2])
endDate = '{}T00:00:00Z'.format(sys.argv[3])
ptype = sys.argv[4]   # LEVEL2A or LEVEL2AP
card = sys.argv[5]    # s2

aoi_query = f"select st_astext(wkb_geometry) from {dbconfig['tables']['aoi_table']} \
  where {dbconfig['args']['aoi_field']} = '{aoi_name}'"

curs.execute(aoi_query)
res = curs.fetchone()

# We expect only one record. Spaces need to be replaced by + to use in subsequent OpenSearch request
aoi=res[0].replace(' ', '+')

print(aoi)
# Query must be one continuous line, without line breaks!!
url = f"https://finder.creodias.eu/resto/api/collections/Sentinel1/search.atom?maxRecords=2000&startDate={startDate}&completionDate={endDate}&productType={ptype}&sortParam=startDate&sortOrder=descending&status=all&geometry={aoi}&dataset=ESA-DATASET"
print(url)
r = requests.get(url)

contentType = r.headers.get('content-type').lower()

# If all goes well, we should get 'application/atom.xml' as contentType
print(contentType)

# PreparedInsert statement
insertSql = f"INSERT into {dbconfig['tables']['catalog_table']} (obstime, reference, sensor, card, footprint) \
    values ('{{}}'::timestamp, '{{}}', '{{}}', '{{}}', ST_GeomFromText('{{}}', 4326))"

if contentType.find('xml')==-1:
    print("FAIL: Server does not return XML content for metadata, but {}.".format(contentType))
    print(r.content)
    sys.exit(1)
else:
    # Some special response handling
    respString = r.content.decode('utf-8').replace('resto:','')
    
    meta = etree.fromstring(bytes(respString, encoding = 'utf-8')) # replace required to avoid namespace bug
    
    entries = meta.xpath('//a:entry', namespaces = {'a' : 'http://www.w3.org/2005/Atom'})
    if len(entries) == 2000:
        print("Likely more than 2000 records, please refine selection arguments")
        sys.exit(1)
    elif len(entries) > 0:
        print(f"{len(entries)} found")
    else:
        print("No entries found")
        sys.exit(1)


# The XML parsing will select relevant metadata parameters and reformats these into records to insert into the __dias_catalogue__ table. 
# Note that rerunning the parsing will skip records that are already in the table with an existing reference attribute (the primary key).
# Rerunning will, thus, only add new records.
# 

for e in entries:
    title = e.xpath('a:title', namespaces = {'a' : 'http://www.w3.org/2005/Atom'})
    #print(title[0].text)
    sensor = title[0].text[1:3].strip()
    #print(sensor)
    tstamp = e.xpath('gml:validTime/gml:TimePeriod/gml:beginPosition', namespaces = {'gml' : 'http://www.opengis.net/gml'})
    #print(tstamp[0].text)
    polygon = e.xpath('.//gml:coordinates', namespaces = {'gml' : 'http://www.opengis.net/gml'})
    try:
        coords = polygon[0].text
        #print(coords)
        footprint = 'POLYGON(({}))'.format(coords.replace(' ',';').replace(',',' ').replace(';',','))
        #print(footprint)
        try:
            curs.execute(insertSql.format(tstamp[0].text.replace('T', ' '), title[0].text, sensor, card, footprint))
            #print(curs.query)
            conn.commit()
        except psycopg2.IntegrityError as e:
            print(e)
            conn.rollback()

    except:
        print("Polygon parsing issues for {} with polygon {}".format(title[0].text, polygon))

# Important attributes in the __dias_catalogue__ table are:
# 
#  - _reference_: this is the unique reference, with which the S3 object storage key to locate the relevant file(s) can be created;
#  - _obstime_: the image acquisition time stamp (UTC);
#  - _sensor_: the sensor (1A, 1B, 2A or 2B);
#  - _card_: this is the CARD type. Together with _sat_ they point to the expected CARD types, but _type_ is unique already;
#  - _footprint_: this is the footprint geometry of the CARD image;
#  
# Note that, for Sentinel-1, we do not store the orbit direction (_ASCENDING_ or _DESCENDING_). As a general rule, UTC time stamps in the (local) morning are for descending orbits, in the evening for ascending orbits.
# 

# Get some statistics on CARD types that are available for this area of interest
getMetadataSql = f"select card, sensor, count(*), min(obstime), max(obstime) from {dbconfig['tables']['catalog_table']},\
    {dbconfig['tables']['aoi_table']} where st_intersects(footprint, wkb_geometry)\
    and {dbconfig['args']['aoi_field']} = '{aoi_name}'\
    group by card, sensor order by card, sensor;"

curs.execute(getMetadataSql)
# Get the columns names for the rows
colnames = [desc[0] for desc in curs.description]
print(colnames)

for rows in curs:
    print(rows[0:3], datetime.strftime(rows[3], '%Y-%m-%d %H:%M:%S'), datetime.strftime(rows[4], '%Y-%m-%d %H:%M:%S'))   
    

# Each record get the _status_ 'ingested' by default. 
# Close database connection

curs.close()
conn.close()
