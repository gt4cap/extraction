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
card = 's2'

# Select for a 14 months growing season, needed for catch crops
startDate = datetime(int(year)-1, 10, 1).strftime('%Y-%m-%d')
endDate = datetime(int(year)+1, 1, 1).strftime('%Y-%m-%d')

# Set up the swarm and deploy the stack
docker.swarm.init(advertise_address='192.168.0.14')

selectSQL = f"""select count(*) from dias_catalogue, aois where card = '{card}' 
      and obstime between '{startDate}' and '{endDate}' and 
      footprint && wkb_geometry and name = '{aoi}'
      and status in ('ingested')"""
    
curs.execute(selectSQL)

r = curs.fetchone()

print(f"{r[0]} entries to be processed")


print("Deploying scl stack")
swarmpit_stack = docker.stack.deploy("scl", compose_files=["./docker-compose_scl.yml"])

while True:
    # check every 2 minutes
    time.sleep(120)

    curs.execute(selectSQL)
    r = curs.fetchone()
    
    if r[0] > 0:
        print(f"{r[0]} entries to be processed")
    else:
        break    
        
print("Stack scl finished")
swarmpit_stack.remove()
print("Stack scl removed")


# Reset the extracted to ingested
updateSQL = f"""update dias_catalogue set status = 'ingested' 
      where id in (select id from dias_catalogue, aois where card = '{card}' 
      and obstime between '{startDate}' and '{endDate}' and 
      footprint && wkb_geometry and name = '{aoi}'
      and status in ('extracted'))"""
      
try:
    curs.execute(updateSQL)
    conn.commit()
except Error as e:
    print("Update failed")
    conn.rollback()
    conn.close()
    sys.exit(1)
         
curs.execute(selectSQL)

r = curs.fetchone()

print(f"{r[0]} entries to be processed")


print("Deploying s210 stack")
swarmpit_stack = docker.stack.deploy("s210", compose_files=["./docker-compose_s210.yml"])

while True:
    # check every 3 minutes
    time.sleep(180)

    curs.execute(selectSQL)
    r = curs.fetchone()
    
    if r[0] > 0:
        print(f"{r[0]} entries to be processed")
    else:
        break    
        
print("Stack s210 finished")
swarmpit_stack.remove()
print("Stack s210 removed")


try:
    curs.execute(updateSQL)
    conn.commit()
except Error as e:
    print("Update failed")
    conn.rollback()
    conn.close()
    sys.exit(1)
    
print("Deploying s220 stack")

swarmpit_stack = docker.stack.deploy("s220", compose_files=["./docker-compose_s220.yml"])

while True:
    # check every 3 minutes
    time.sleep(180)

    curs.execute(selectSQL)
    r = curs.fetchone()
    
    if r[0] > 0:
        print(f"{r[0]} entries to be processed")
    else:
        break    
        
print("Stack s220 finished")
swarmpit_stack.remove()
print("Stack s220 removed")
docker.swarm.leave(force=True)
print("Swarm left")




