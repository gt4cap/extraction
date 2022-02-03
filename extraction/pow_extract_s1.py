import sys
import time

from python_on_whales import docker
import psycopg2

from datetime import datetime

# Set up database connection and test
conn_str = "host='185.178.86.9' dbname='outreach' user='postgres' password='db4CbM_2021'"

conn = psycopg2.connect(conn_str)
curs = conn.cursor()

aoi = sys.argv[1]
year = aoi.split('_')[1]
card = 'bs'

# Select for a 14 months growing season, needed for catch crops
startDate = datetime(int(year)-1, 10, 1).strftime('%Y-%m-%d')
endDate = datetime(int(year)+1, 1, 1).strftime('%Y-%m-%d')

selectSQL = f"""select count(*) from dias_catalogue, aois where card = '{card}' 
      and obstime between '{startDate}' and '{endDate}' and 
      footprint && wkb_geometry and name = '{aoi}'
      and status in ('ingested')"""
    
curs.execute(selectSQL)

r = curs.fetchone()

print(f"{r[0]} entries to be processed")

# Set up the swarm and deploy the stack
docker.swarm.init(advertise_address='192.168.0.14')

print("Deploying bs stack")
swarmpit_stack = docker.stack.deploy("bs", compose_files=["./docker-compose_s1_bs.yml"])

sleep = 300
while r[0] > 0:
    # check every 5 minutes
    time.sleep(sleep)

    curs.execute(selectSQL)
    r = curs.fetchone()
    
    if r[0] > 0:
        print(f"{r[0]} entries to be processed")
    elif r[0] < 5:
        sleep = 60
    else:
        break    
        
print("Stack bs finished")
swarmpit_stack.remove()
print("Stack bs removed")

card = 'c6'

selectSQL = f"""select count(*) from dias_catalogue, aois where card = '{card}'
      and obstime between '{startDate}' and '{endDate}' and
      footprint && wkb_geometry and name = '{aoi}'
      and status in ('ingested')"""

curs.execute(selectSQL)

r = curs.fetchone()

print(f"{r[0]} entries to be processed")

print("Deploying c6 stack")

swarmpit_stack = docker.stack.deploy("c6", compose_files=["./docker-compose_s1_c6.yml"])

sleep = 300
while r[0]>0:
    # check every 5 minutes
    time.sleep(sleep)

    curs.execute(selectSQL)
    r = curs.fetchone()
    
    if r[0] > 0:
        print(f"{r[0]} entries to be processed")
    elif r[0] < 10:
        sleep = 60
    else:
        break    
        
print("Stack c6 finished")
swarmpit_stack.remove()
print("Stack c6 removed")
docker.swarm.leave(force=True)
print("Swarm left")
#sys.exit(0)



