# EIA_OMS_DM
PL/SQL Scripts for creating and populating EIA OMS Data Mart
First load must be done to OMS_TEMP_RMS_YYYY (not partitioned), otherwize attributes correction will not work. After load is done and attributes corrected, OMS_TEMP_RMS_XXXX must be copied into OMS_TEMP_RMS_PART_XXXX (partitioned)
I. OMS
 1. DDL for creating table OMS_TEMP_RMS_PART, view OMS_TEMP, materialized view log on OMS_TEMP_RMS_PART, materialized view OMS_MV, view OMS_T
 2. PL/SQL with loop for initial load (1st year 200709-200809 where Oracle ADM is used for all snapshots and Demand Sales for all months)
 3. PL/SQL with loop for regular loads: OMS_TEMP_RMS (target table itself) is used for PY Snapshot and for 5 preceding months OMS sales (instead of Demand Sales) - this speeds up processing and helps identify FOAs (ADM Snapshot doesn't contain FOAs)
 4. PL/SQL with loop for correcting IMC attributes basing on neighbour months - to run after initial loads - normally is not required in monthly process
 5. New partition must be explicitly created before adding a new month (i.e. no partitions created "in advance" with the initial DDL)
II. OMS_MKTG
 1. DDL for OMS_MKTG_TEMP
 2. Scripts for monthly updates of OMS_MKTG_TEMP and OMS_MKTG - the same scripts are used for initial loads and monthly updates
 3. In OMS_MKTG_TEMP, new partition must be explicitly created before adding a new month (i.e. no partitions created "in advance" with the initial DDL)

Order of running updates:

1. OMS_TEMP_RMS_PART
2. OMS_MKTG_TEMP
3. OMS_MKTG


Steps for updating OMS_TEMP_RMS_PART ouside of the scheduled update dates in case of updated which affect long periods (more than 1 year):

1.	Ensure that Sales Dashboard doesn’t have a scheduled refresh for the period during which OMS_TEMP_RMS_PART will be under maintenance
2.	Create a staging table with the same structure as OMS_TEMP_RMS_PART for temporary storing new data, not necessarily partitioned
3.	Insert new data (which must replace data in OMS_TEMP_RMS_PART) into staging table: first 12 months must be inserted with the script for initial load, the rest with the script for current data flow
4.	Run correctional script on the new table to assign attributes for ABOs who were not in the snapshot
5.	Drop Materialized view log on OMS_TEMP_RMS_PART and materialized view OMS_MV
6.	Truncate all partitions (or subpartitions, depending on update) which must be reloaded, in OMS_TEMP_RMS_PART
7.	Insert data into OMS_TEMP_RMS_PART from the staging table
8.	Compress OMS_TEMP_RMS_PART
9.	Gather statistics om OMS_TEMP_RMS_PART
10.	Recreate Materialized view log
11.	Recreate materialized view
12.	Drop staging table
13.	Purge Recycle bin
14.	Recompile all dependent views (ALTER VIEW… COMPILE)
15.	Recover refresh schedule for Sales Dashboard if necessary

If necessary, update marketing tables after this
