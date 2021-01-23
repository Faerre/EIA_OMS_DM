# EIA_OMS_DM
PL/SQL Scripts for creating and populating EIA OMS Data Mart
First load must be done to OMS_TEMP_RMS_YYYY (not partitioned), otherwize attributes correction will not work. After load is done and attributes corrected, OMS_TEMP_RMS_XXXX must be copied into OMS_TEMP_RMS_PART_XXXX (partitioned)
I. OMS_TEMP_RMS_(PART)
 1. DDL for creating table
 2. PL/SQL with loop for initial load (1st year 200709-200809 where Oracle ADM is used for all snapshots and Demand Sales for all months)
 3. PL/SQL with loop for regular loads: OMS_TEMP_RMS (target table itself) is used for PY Snapshot and for 5 preceding months OMS sales (instead of Demand Sales) - this speeds on processing and helps identify FOAs (ADM Snapshot doesn't contain FOAs)
 4. PL/SQL with loop for correcting IMC attributes basing on neighbour months - to run after initial loads - normally is not required in monthly process
 5. Marketing - TBD
