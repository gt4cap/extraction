# Onboarding a Member State on DIAS

## Account set up
An Onboarding MS needs a DIAS account. For the account, a contact person (name, email, phone) has to be identified.

ESA is asked to forward these contact details to the DIAS provider with the explicit authorization to activate the account.

The DIAS provider confirms, sets up the account and provides the MS contact with the relevant details. The DIAS provider has to allocated credits to the account, so that resources can be used (needs to be confirmed!)

## VM set up

A tenant VM is not set up with account creation. This needs to be done via the [Horizon/Openstack GUI](https://cf2.cloudferro.com/project/) after login)

If done by MS (see [Essential steps](#essential-steps) below):

For hands-on support to the Member State, JRC needs to have ssh access to the account. JRC provides the public key that is used on it's own DIAS resources.
The public key needs to be appended to the ~/.ssh/authorized_keys file on the MS VM.
JRC to confirm ssh access.

**Alternative**: JRC to set up VM with MS credentials

### Essential steps

Create SSH key pair during VM creation. Copy the **PRIVATE** key to a local ~/.ssh/keys/{name}.key file (chmod 0600 {name}.key). The PUBLIC key will be copied to the new VM instance.

Select a machine type that can handle extraction on a single box (KISS). An 8 vCPU with 16 GB RAM is OK to run parallel extraction for cases with up to 1 million features, parallel to the database server.

Add a volume (for db). See detailed instructions on [the CREODIAS FAQ page](https://creodias.eu/-/how-to-attach-a-volume-to-vm-2-tb-linux-?inheritRedirect=true&redirect=%2Ffaq-data-volume).

The VM needs to be created with sufficient disk space to run the database. Rule of thumb: 100 GB disk space for 1 Million parcels for one year volume (all S1 and S2)

We assume mount volume is at /data

Create a new Security group that allows Ingress on a non-standard port (e.g. 11039) and add to instance. This is a simple security provision to complicate port specific hacks.

## Configure VM

On any new VM start with:

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install vim
```
(cannot be run unattended if kernel update!!)

vi is needed for minimal text file editing.

Docker install (also on any VM if parallel work in expected)

```bash
sudo apt install docker.io
sudo usermod -aG docker $USER
```

Logout and login to take effect.

## Installing the database container

PostgreSQL/postgis will run in a docker container.

Since the database needs access to a large volume, this requires a re-direction of the docker repository (normally on ```/var/lib/docker```) to ```/data/docker```

Create docker volume on mounted disk partition. See [instructions](https://www.guguweb.com/2019/02/07/how-to-move-docker-data-directory-to-another-location-on-ubuntu/)

```bash
sudo service docker stop
sudo vi /etc/docker/daemon.json
```
Add the following in daemon.json
```json
{
  "data-root": "/data/docker"
}
```
and

```bash
sudo rsync -aP /var/lib/docker/ /data/docker
sudo mv /var/lib/docker /var/lib/docker.old
sudo service docker start
```
Check whether docker runs OK. If so:

```bash
sudo rm -Rf /var/lib/docker.old
```

First create a volume for the database and pull a postgis image
```bash
docker volume create database
docker pull mdillon/postgis:latest
```

Do not use ```kartoza/postgis```, because it leads to a convoluted installation, with 200 MB of useless locales. Start a container using the ```mdillon/postgis image```:

```bash
docker run --name ams_db -d --restart always -v database:/var/lib/postgresql --shm-size=2gb -p 11039:5432 mdillon/postgis
```

The port redirect (-p 11039:5432) uses the port that was opened at VM installation.

You need to login into the new container to set the ```pg_hba.conf``` to allow connections via the network. You are sudo inside the container

```bash
docker exec -it ams_db /bin/bash

apt update
apt upgrade -y
apt install vim
vi /var/lib/postgresql/data/pg_hba.conf
```

Change all permissions from ```trust``` to ```md5```, except the local ones.
Network connection already set to '\*' in ```postgresql.conf```.

Exit the container and commit to an updated image (so as to not loose the updates)

On the host VM, the following sequence will:
- list the running containers (```mdillon/postgis``` is in daemon mode)
- commits the updated container to a new image
- list all images (which should show the new one next to the ```mdillon/postgis:latest```)
- tags the committed image with the name ```mdillon/postgis:updated```
- removes the running ```mdillon/postgis:latest``` daemon container, making sure the mounted database volume is safely unmounted
- starts the updated ```mdillon/postgis:updated``` as a new daemon container, using the database volume and the same db name

```bash
docker ps -a
docker commit <container-id>
docker images
docker tag <image-id> mdillon/postgis:updated
docker rm -vf ams_db
docker run --name ams_db -d --restart always -v database:/var/lib/postgresql --shm-size=2gb -p 11039:5432 mdillon/postgis:updated
```

## Configure the database

Since the VM itself has no postgresql clients installed (yet), access the database from within the new container. Inside the container, you are a local user, so do not need a password. Remove the pre-installed schema that come with ```mdillon/postgis``` and set a password for the postgres user.

```bash
docker exec -it ams_db /bin/bash
psql -U postgres
drop schema tiger cascade;
drop schema tiger_data cascade;
drop schema topology cascade;
alter role postgres with password 'YOURPASSWORD';
```

One can now communicate with the postgresql database from external machines, but only with the password.

### Stuff the GSAA shape file

Upload the shape set into dk2021 table. This can be done from a remote machine. Make sure to pass in the spatial reference system code and default MULTIPOLYGON type.

Test with QGIS connection to database.

```bash
ogr2ogr -f "PostgreSQL" PG:"host=185.52.195.114 port=11039 dbname=postgres user=postgres password=YOURPASSWORD" -nln dk2021 -a_srs EPSG:25832 -nlt PROMOTE_TO_MULTI Endelige_marker_til_DHI_2021.sh
```

### Set up required tables

The aois table holds the definition of the Area Of Interest, which is the outline of the area for which CARD data needs to be processed. This can be generated from the extent of the parcel set.

```postgresql
create table aois (name text);
select addgeometrycolumn('aois', 'wkb_geometry', 4326, 'POLYGON', 2);
insert into aois values ('dk2021', (select st_transform(st_extent(wkb_geometry), 4326) from dk2021));
```

Metadata for CARD data needs to be transferred from the DIAS catalog to the database. The cross-section of parcels and CARD data sets is stored in the hists table (cloud-occurence statistics for Sentinel-2 L2A) and sigs table (all bands extracts for Sentinel-2 L2A, Sentinel-1 CARD-BS and Sentinel-1 CARD-COH6).

Create the tables:

```postgresql
CREATE TABLE public.dias_catalogue (
    id serial NOT NULL,
    obstime timestamp without time zone NOT NULL,
    reference character varying(120) NOT NULL,
    sensor character(2) NOT NULL,
    card character(2) NOT NULL,
    status character varying(24) DEFAULT 'ingested'::character varying NOT NULL,
    footprint public.geometry(Polygon,4326)
);

ALTER TABLE ONLY public.dias_catalogue
    ADD CONSTRAINT dias_catalogue_pkey1 PRIMARY KEY (id);

CREATE INDEX dias_catalogue_footprint_idx ON public.dias_catalogue_eosc2019 USING gist (footprint);

CREATE UNIQUE INDEX dias_catalogue_reference_idx ON public.dias_catalogue_eosc2019 USING btree (reference);
--

CREATE TABLE public.sigs (
    pid integer,
    obsid integer,
    band character(3),
    count real, mean real, std real,
    min real, max real,
    p25 real, p50 real, p75 real
);

CREATE INDEX sigs_bidx ON public.sigs USING btree (band);
CREATE INDEX sigs_obsidx ON public.sigs USING btree (obsid);
CREATE INDEX sigs_pidx ON public.sigs USING btree (pid);

CREATE TABLE public.hists (
    pid integer,
    obsid integer,
    hist json
);

CREATE INDEX hists_obsidx ON public.hists USING btree (obsid);
CREATE INDEX hists_pidx ON public.hists USING btree (pid);
```

## Setting up extraction

### Pull dias_numba_py

All python dependencies for fast extraction are packaged in the glemoine62/dias_numba_py docker image. Thus, all extraction routine will be run from within a derived container.

```bash
docker pull glemoine62/dias_numba_py:latest
```

### git install catalog and extraction code

The latest extraction code is currently on the **PRIVATE** gt4cap extraction repository (only dev_backend users have access for now). It will migrate to the public cbm repository at some point.

Install the code on the new VM  

```bash
mkdir cbm
cd cbm
git init
git remote add origin https://github.com/gt4cap/extraction.git
git pull origin main
```

### Transfer records from finder.creodias.eu

The metadata of Sentinel CARD needs to be transcribed into the dias_catalogue table. This is done via scripts that parse the XML output of the OpenSearch requests to [the CREODIAS catalog](https://finder.creodias.eu) for the respective S-2 and S-1 data sets.

In the folder cbm/catalog update the cat_config.json as follows (using the docker network to connect to the container):

```json
{
	"database": {
		"connection": {
			"host": "172.17.0.2",
			"dbname": "postgres",
			"dbuser": "postgres",
			"dbpasswd": "YOURPASSWORD",
			"port": 5432
		},
		"tables": {
			"aoi_table": "aois",
			"catalog_table": "dias_catalogue"
		},
		"args": {
			"aoi_field": "name"
		}
	}
}
```

Get the S-2 Level 2A data. Note that the finder.creodias.eu response can have a **maximum of 2000 records** per request. This means that the selection parameters need to be tuned to return less than 2000 records. Since the aoi is fixed, this can only be done by limiting the start and end date parameters.

For DK, run the Sentinel-2 selection over 3 months periods (for the entire date range). Check whether less than 2000 records are found (if 2000, adapt the range to a shorter period).

For Sentinel-1 the total number of scenes is typically lower (much larger footprints), so longer periods can be used.

```bash
docker run -it --rm -v`pwd`:/usr/src/app glemoine62/dias_numba_py python creodiasCARDS2MetaXfer2DB.py dk2021 2021-09-01 2022-01-01 LEVEL2A s2
POLYGON((8.10435443157384+54.5929343210099,8.02765204689278+57.7516361521132,15.5731994285472+57.5842750710105,15.0586134693368+54.4442190145486,8.10435443157384+54.5929343210099))
application/atom+xml
1761 found

docker run -it --rm -v`pwd`:/usr/src/app glemoine62/dias_numba_py python creodiasCARDS1MetaXfer2DB.py dk2021 2020-10-01 2022-01-01 CARD-BS bs
POLYGON((8.10435443157384+54.5929343210099,8.02765204689278+57.7516361521132,15.5731994285472+57.5842750710105,15.0586134693368+54.4442190145486,8.10435443157384+54.5929343210099))
application/atom+xml
666 found

docker run -it --rm -v`pwd`:/usr/src/app glemoine62/dias_numba_py python creodiasCARDS1MetaXfer2DB.py dk2021 2020-10-01 2022-01-01 CARD-COH6 c6
POLYGON((8.10435443157384+54.5929343210099,8.02765204689278+57.7516361521132,15.5731994285472+57.5842750710105,15.0586134693368+54.4442190145486,8.10435443157384+54.5929343210099))
application/atom+xml
337 found

['card', 'sensor', 'count', 'min', 'max']
('bs', '1A', 320) 2020-10-02 05:32:37 2021-02-19 17:17:31
('bs', '1B', 346) 2020-10-01 05:40:09 2021-12-16 16:28:13
('c6', '1A', 181) 2020-10-02 05:32:35 2021-12-28 17:17:38
('c6', '1B', 156) 2020-10-01 05:40:08 2020-12-31 16:44:48
('s2', '2A', 3341) 2020-10-01 10:30:31 2021-12-31 10:54:41
('s2', '2B', 3392) 2020-10-02 10:47:59 2021-12-30 10:33:39
```

At the current stage, all Sentinel-2 L2A is available, but only a subset of Sentinel-1 CARD-BS and CARD-COH6, since no specific orders have been made (the available scenes are "spill over" for other actions run on CREODIAS).

### Check which UTM projections are in the CARD data sets

Parcel extraction uses rasterized versions of the parcel features. These need to be generated (once) for the UTM projections and resolutions (10, 20 m) of the CARD data. The UTM projections over the AOI can be retrieved from the Sentinel-2 L2A image names, which are stored as reference in the dias_catalogue

```postgresql
select distinct substr(split_part(reference, '_', 6),2,2)::int from dias_catalogue, aois where footprint && wkb_geometry and card = 's2';
```

(returns 32 and 33)

### Create raster versions of the parcels in required projection and resolution

```postgresql
create table dk2021_32632_10_rast as (select ogc_fid pid, st_asraster(st_transform(wkb_geometry, 32632), 10.0, 10.0, '8BUI') rast from dk2021);
alter table dk2021_32632_10_rast add primary key(pid);
```

(repeat for every other combination of UTM projection and resolution)

There is no need for a spatial index on the rast field, because all spatial selection are done on the original parcel feature table.

Make sure that the table naming convention {aoi}_{UTM}_{res}_rast is stricly adhered to (will be used in extraction code).

Houston, everything ready for extraction!

# Running extraction

Extraction code is on cbm/extraction. The directory data is needed for temporary storage of VRT files:

```bash
mkdir data
```

## Single runs

Executables takes their parameters from a runtime configuration file. Note that database tables need to specify the schema. The docker section defines the address of the swarm master.

```json
{
    "database": {
        "connection": {
            "host": "185.52.195.114",
            "dbname": "postgres",
            "dbuser": "postgres",
            "dbpasswd": "YOURPASSWORD",
            "port": 11039
        },
        "tables": {
            "aoi_table": "public.aois",
            "parcel_table": "public.dk2021",
            "catalog_table": "public.dias_catalogue",
            "sigs_table": "public.sigs",
            "hists_table": "public.hists"
        },
        "args": {
            "aoi_field": "name",
            "name": "dk2021",
            "startdate": "2020-10-01",
            "enddate": "2022-01-01"
        }
    },
    "docker": {
        "masterip": "192.168.0.8"
    }
}
```

For Sentinel-2 L2A, the order of execution is to first extract the SCL histograms and then the 10 m and 20 m band extracts. The SCL histogram run identifies S2 granules that have no parcels. These are then excluded in successive runs.

The order of the Sentinel-1 CARD-BS and CARD-COH6 runs is not important.

A single histogram extraction run is executed as follows:

```
docker run -it --rm -v`pwd`:/usr/src/app -v/eodata:/eodata -v/1/DIAS:/1/DIAS glemoine62/dias_numba_py python factoredWindowedExtraction.py s2 -1
```

For the 10 m band run, change the argument -1 to 10, for 20 m band runs, change to 20.

Note that **BOTH** the /eodata and /1/DIAS volumes need to be mounted by the container. The latter is a local cache where all data read from S3 is stored. This needs to be cleared after each run (done inside the code).

Extraction manipulates the status field of the dias_catalogue, as follows:
- After [catalogue transfer](#Transfer-records-from-findercreodiaseu) all new images are marked with status 'ingested'
- At the start of extraction an image candidate will be marked as 'inprogress'
- If no parcels are found in the image footprint, status is changed to 'No parcels'
- If extraction fails (for a variety of reasons) status is changed to a meaningful error status (e.g. 'No in db', 'Rio error', 'Parcel SQL') which can be traced to the relevant code fragment.
- If extraction completes without error for a set of parcels, status is changed to 'extracted'
- Extraction exits after a particular status is reached.

Check status statistics in the database:

```
select card, status, count(*) from dias_catalogue group by card, status;
```

## docker stack runs

In order to benefit from parallel processing, the easiest way is to use docker stack, even on a single machine (multiple VM swarms are for later).

The stack requires a docker compose configuration as follows:

```yaml
version: '3.5'
services:
  vector_extractor:
    image: glemoine62/dias_numba_py:latest
    volumes:
            - /home/eouser/cbm/extraction:/usr/src/app
            - /eodata:/eodata
            - /1/DIAS:/1/DIAS
    deploy:
      replicas: 4
    command: python factoredWindowedExtraction.py s2 -1
```

and can be deployed as follows:

```bash
docker stack deploy -c docker-compose_scl.yml
```

This will start 4 parallel processes that run the histogram extraction.

You can check the output of the processes with:

```bash
docker service ls
docker service logs -f scl_vector_extractor
```

The first command list the running services, the second log details of the extraction processes.

A drawback of running a stack (in this context) is that, after all ingested images are processed, the stack needs to be terminated manually. It will simply continue to launch processes that will not find 'ingested' candidates and exit.

The stack can be removed as follows:

```bash
docker stack rm scl
```

## python_on_whales runs

A solution to the stack termination issue is to integrate stack deployment with logic that can tear down the stack after checking 'ingested' status in the database. This can, in principle, be done in the bash shell (e.g. running a background process). A slightly more elegant solution is with python_on_whales, which controls docker processes from within python.

A slight drawback is the need to install some python modules on the VM:

```bash
sudo cp /usr/bin/python3 /usr/bin/python
sudo apt install python3-pip
pip3 install psycopg2-binary
pip3 install python_on_whales
```

The script ```pow_extract_s2.py``` runs the Sentinel-2 L2A extraction in a series of 3 stack deployments. The script checks the database for 'ingested' candidates at regular intervals and removes the relevant stack when exhausted. For successive runs, it will set the 'extracted' candidates to 'ingested' and starts the next stack.

Run as a background process (so you can log out of the VM):

```
nohup python pow_extract_s2.py &
```

The ```pow_extract_s1.py``` is the equivalent for Sentinel-1 CARD extraction.

## Post extraction checks

Extraction may run for several days, depending on archive size and number of parcel features. At the end of the extraction run, some images may have been left in 'inprogress' status. This may be because of a dropped database connection, or for some other reason.

The 'inprogress' status may have been reached after a subset of parcels were already extracted. Thus, it is best to clean out the hists and sigs tables and redo the extraction. Save the 'inprogress' records to a separate table.

```postgresql
create table faulties as (select id from dias_catalogue where status = 'inprogress' and card ='s2');

delete from hists where obsid in (select id from faulties);
delete from sigs where obsid in (select id from faulties);
update dias_catalogue set status = 'ingested' where status = 'inprogress';
```

**DO NOT USE** the pow_extract_s2.py script, because it will reset ALL extracted status to ingested for the second stack run!!!

Instead, run as many [individual runs](#Single-runs) as the number of 'inprogress' records. Use a bash script, if needed. Start with the SCL extraction to histograms. After it has finished, reset:

```postgresql
update dias_catalogue set status = 'ingested' where id in (select id from faulties)
```

Run the extraction for 10 m bands, reset again, run the 20 m bands and wrap up:

```postgresql
drop table faulties;
```

## Vacuum and cluster

The hists and sigs hold many millions of records after extraction. Both are indexed on the parcel id (pid) and observation id (obsid). The sigs table is also indexed on band. Since the typical access pattern is expected to be based on the parcel id, the tables are clustered on the pid index.

```postgresql
vacuum analyze hists;
cluster hists using (hists_pidx);
vacuum analyze sigs;
cluster sigs using (sigs_pidx)
```

This will take considerable time. Clustering can only be performed if sufficient disk space is available as each table is rewritten in its entirety.


# Maintenance

## Deleting fully cloud covered parcel observations

If database size is an issue, one easy reduction step is to delete all sigs records for which parcels are fully cloud covered (SCL value 9):

```postgresql
with cnt_keys as (select pid, obsid, (select count(*) from jsonb_object_keys(hist::jsonb)) from hists where hist::jsonb ? '9')  select * into cloudy from cnt_keys where count = 1;
SELECT 49879887
postgres=# select count(*) from hists;
   count   
-----------
 131393590
```

i.e. almost 38% of all sigs records can be removed (~ 21 GB) without significant loss of information (the cloud cover information is not removed).


## table backup with pg_dump
## adding bands and/or indices
## automate extraction
## what else
