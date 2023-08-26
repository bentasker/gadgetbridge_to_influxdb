#!/usr/bin/env python3
#
#
# Fetch a Gadgetbridge database export from a WebDAV URL
# (in my case, Nextcloud) and then extract stats to write
# onwards into InfluxDB.
#
# Copyright (c) 2023, B Tasker
# Released under BSD 3-clause
#
#
# pip install webdavclient3 influxdb-client
'''
Copyright 2023 B Tasker

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

    Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

import os
import shutil
import sqlite3
import sys
import tempfile
import time
from webdav3.client import Client
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


### Config section

# This expects hostname and scheme
#
# For nextcloud it'll be https://[nextcloud domain]/remote.php/dav/
WEBDAV_URL = os.getenv("WEBDAV_URL", False)

# Path to the export file
WEBDAV_PATH = os.getenv("WEBDAV_PATH", "files/service_user/GadgetBridge/")

# Creds
WEBDAV_USER =  os.getenv("WEBDAV_USER", False)
WEBDAV_PASS =  os.getenv("WEBDAV_PASS", False)

# What's the filename of the file on the webdav server?
EXPORT_FILE = os.getenv("EXPORT_FILENAME", "gadgetbridge")

# How far back in time should we query when extracting stats?
QUERY_DURATION = int(os.getenv("QUERY_DURATION", 7200))

# InfluxDB settings
INFLUXDB_URL = os.getenv("INFLUXDB_URL", False)
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT", "gadgetbridge")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "testing_db")

# Which hours should be considered sleeping hours?
# utilities/gadgetbridge_to_influxdb#6
SLEEP_HOURS = os.getenv("SLEEP_HOURS", "0,1,2,3,4,5,6").split(",")


### Config ends


def fetch_database(webdav_client):
    ''' Connect to the WebDAV server and fetch the named database
    file, if it exists.
    
    '''
    file_list = webdav_client.list(WEBDAV_PATH)
    if EXPORT_FILE in file_list:
        info = webdav_client.info(f'{WEBDAV_PATH}/{EXPORT_FILE}')
    else:
        print("Error: Export file does not exist")
        sys.exit(1)
        
    # Create a temporary directory to operate from
    tempdir = tempfile.mkdtemp()
    print(tempdir)
    # Download the file
    webdav_client.download_sync(remote_path=f'{WEBDAV_PATH}/{EXPORT_FILE}', local_path=f'{tempdir}/gadgetbridge.sqlite')
    
    return tempdir


def open_database(tempdir):
    ''' Open a handle on the database
    '''
    conn = sqlite3.connect(f"{tempdir}/gadgetbridge.sqlite")
    cur = conn.cursor()
    
    return conn, cur


def extract_data(cur):
    ''' Query the database for data
    '''
    results = []
    devices = {}
    devices_observed = {}
    query_start_bound = int(time.time()) - QUERY_DURATION

    # Pull out device names
    device_query = "select _id, NAME from DEVICE"
    try:
        res = cur.execute(device_query)
    except sqlite3.OperationalError as e: 
        # We received an empty db
        print("Unable to fetch stats - received an empty database")
        return False
    
    for r in res.fetchall():
        devices[f"dev-{r[0]}"] = r[1]

    # Get SpO2 info
    spo2_data_query = ("SELECT TIMESTAMP, DEVICE_ID, TYPE_NUM, SPO2 FROM HUAMI_SPO2_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(spo2_data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000000 # Convert to nanos
        row = {
                "timestamp": row_ts, 
                "fields" : {
                    "spo2" : r[3]
                    },
                "tags" : {
                    "type_num" : r[2],
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts
    
    stress_data_query = ("SELECT TIMESTAMP, DEVICE_ID, TYPE_NUM, STRESS FROM HUAMI_STRESS_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(stress_data_query)
    for r in res.fetchall():
        # Note, the timestamps for these items in the SQliteDB are in ms not S
        row_ts = r[0] * 1000000
        row = {
                "timestamp": row_ts, 
                "fields" : {
                    "stress" : r[3]
                    },
                "tags" : {
                    "type_num" : r[2],
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        
        
        # Convert the timestamp to a time object so that we can check what hour of
        # the day it currently represents
        # 
        # If it's outside of sleeping hours we'll add a field
        #
        # utilities/gadgetbridge_to_influxdb#6
        if time.gmtime(r[0] / 1000).tm_hour not in SLEEP_HOURS:
            row['fields']['stress_exc_sleep'] = r[3]
        
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts        
    
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, RATE FROM HUAMI_SLEEP_RESPIRATORY_RATE_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        # I don't currently have any data examples of this, but I assume it will be in ms
        # the saame as the other HUAMI_*SAMPLE entries
        row_ts = r[0] * 1000000
        row = {
                "timestamp": row_ts, # Convert to nanos
                "fields" : {
                    "sleep_respiratory_rate" : r[2]
                    },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts                


    data_query = ("SELECT TIMESTAMP, DEVICE_ID, PAI_LOW, PAI_MODERATE, PAI_HIGH, TIME_LOW," 
        "TIME_MODERATE, TIME_HIGH, PAI_TODAY, PAI_TOTAL "
        "FROM HUAMI_PAI_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        # Note, the timestamps for these items in the SQliteDB are in ms not S
        row_ts = r[0] * 1000000
        row = {
                "timestamp": row_ts, # Convert to nanos
                "fields" : {
                    "pai_low" : r[2],
                    "pai_moderate" : r[3],
                    "pai_high" : r[4],
                    "time_low" : r[5],
                    "time_moderate" : r[6],
                    "time_high" : r[7],
                    "pai_today" : r[8],
                    "pai_total" : r[9]
                    },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)    
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts              

    data_query = ("SELECT TIMESTAMP, DEVICE_ID, LEVEL, BATTERY_INDEX FROM BATTERY_LEVEL "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000000
        row = {
                "timestamp": row_ts ,
                "fields" : {
                    "battery_level" : r[2]
                    },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"],
                    "battery" : r[3]
                    }
            }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts         


    # Heart rates are spread across tables, depending on the sampling types
    rate_types = {
        "manual" : "HUAMI_HEART_RATE_MANUAL_SAMPLE",
        "max" : "HUAMI_HEART_RATE_MAX_SAMPLE",
        "resting" : "HUAMI_HEART_RATE_RESTING_SAMPLE"
        }
    
    for rate_type in rate_types:
        data_query = (f"SELECT TIMESTAMP, DEVICE_ID, HEART_RATE FROM {rate_types[rate_type]} "
            f"WHERE TIMESTAMP >= {query_start_bound} "
            "ORDER BY TIMESTAMP ASC")
        res = cur.execute(data_query)
        for r in res.fetchall():
            # I don't currently have any data examples of this, but I assume it will be in ms
            # the saame as the other HUAMI_*SAMPLE entries            
            row_ts = r[0] * 1000000
            row = {
                    "timestamp": row_ts, # Convert to nanos
                    "fields" : {
                        "heart_rate" : r[2]
                        },
                    "tags" : {
                        "device" : devices[f"dev-{r[1]}"],
                        "sample_type" : rate_type
                        }
                }
            results.append(row)
            if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
                devices_observed[f"dev-{r[1]}"] = row_ts
        
    # Get values from the activity table
    #
    # Activity types are deliniated by the value of RAW_KIND
    # but there isn't currently a reliable mapping for the 
    # meaning of each. There are also suggestions online that
    # the meanings sometimes change between firmware revisions
    # 
    # So, we'll just expose the value as a tag rather than attempting
    # to map it to anything
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, RAW_INTENSITY, STEPS, RAW_KIND, HEART_RATE, SLEEP,"
        "DEEP_SLEEP, REM_SLEEP FROM HUAMI_EXTENDED_ACTIVITY_SAMPLE " 
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        # I don't currently have any data examples of this, but I assume it will be in ms
        # the saame as the other HUAMI_*SAMPLE entries
        row_ts = r[0] * 1000000
        row = {
                "timestamp": row_ts, # Convert to nanos
                "fields" : {
                    "intensity" : r[2],
                    "steps" : r[3],
                    "heart_rate" : r[5],
                    "sleep" : r[6],
                    "deep_sleep" : r[7],
                    "rem_sleep" : r[8],
                    },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"],
                    "activity_kind" : r[4],
                    "sample_type" : "activity"
                    }
            }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts        

    # Get normal steps and HR measurements
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, RAW_INTENSITY, STEPS, RAW_KIND, HEART_RATE"
        " FROM MI_BAND_ACTIVITY_SAMPLE " 
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")    

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000000
        row = {
                "timestamp": row_ts, # Convert to nanos
                "fields" : {
                    "intensity" : r[2],
                    "steps" : r[3],
                    "raw_intensity" : r[2],
                    "raw_kind" : r[4]
                    },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"],
                    "sample_type" : "periodic_samples"
                    }
            }

        results.append(row)     
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts           

    # Create a field to record when we last synced, based on the values in devices_observed
    now = time.time_ns()
    for device in devices_observed:
        row_ts = devices_observed[device]
        row_age = now - row_ts
        row = {
                "timestamp": now,
                "fields" : {
                    "last_seen" : row_ts,
                    "last_seen_age" : row_age
                    },
                "tags" : {
                    "device" : devices[device],
                    "sample_type" : "sync_check"
                    }
            }
        print(row)
        results.append(row)   

    return results


def write_results(results):
    ''' Open a connection to InfluxDB and write the results in
    '''

    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as _client:
        with _client.write_api() as _write_client:
            # Iterate through the results generating and writing points
            for row in results:
                p = Point(INFLUXDB_MEASUREMENT)
                for tag in row['tags']:
                    p = p.tag(tag, row['tags'][tag])
                    
                for field in row['fields']:
                    if row['fields'][field] == -1:
                        continue
                    
                    # Skip any special heart_rate values
                    # utilities/gadgetbridge_to_influxdb#1
                    if field == "heart_rate" and row['fields'][field] > 253:
                        continue
                    
                    p = p.field(field, row['fields'][field])
                    
                p = p.time(row['timestamp'])
                _write_client.write(INFLUXDB_BUCKET, INFLUXDB_ORG, p)
                
    

if __name__ == "__main__":
    if not WEBDAV_URL:
        print("Error: WEBDAV_URL not set in environment")
        sys.exit(1)

    if not INFLUXDB_URL:
        print("Error: INFLUXDB_URL not set in environment")
        sys.exit(1)

    webdav_options = {
        "webdav_hostname" : WEBDAV_URL,
        "webdav_login" : WEBDAV_USER,
        "webdav_password" : WEBDAV_PASS
        }

    webdav_client = Client(webdav_options)
    tempdir = fetch_database(webdav_client)

    conn, cur  = open_database(tempdir)

    # Extract data from the DB
    results = extract_data(cur)
    if not results:
        print("Data extraction failed")
        sys.exit(1)

    # Write out to InfluxDB
    write_results(results)
    
    # Tidy up
    conn.close()
    #if tempdir not in ["/", ""]:
    #    shutil.rmtree(tempdir)
