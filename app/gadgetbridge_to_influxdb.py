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
# pip install webdavclient3
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



# This expects hostname and scheme
#
# For nextcloud it'll be https://[nextcloud domain]"
WEBDAV_URL = os.getenv("WEBDAV_URL", False)
# Path to the export file
WEBDAV_PATH = os.getenv("WEBDAV_PATH", "files/service_user/GadgetBridge/")

# Creds
WEBDAV_USER =  os.getenv("WEBDAV_USER", False)
WEBDAV_PASS =  os.getenv("WEBDAV_PASS", False)

EXPORT_FILE = os.getenv("EXPORT_FILENAME", "gadgetbridge")


def fetch_database(webdav_client):
    file_list = webdav_client.list(WEBDAV_PATH)
    print(file_list)
    if EXPORT_FILE in file_list:
        info = webdav_client.info(f'{WEBDAV_PATH}/{EXPORT_FILE}')
    else:
        print("Error: Export file does not exist")
        sys.exit(1)
        
    # Create a temporary directory to operate from
    tempdir = tempfile.mkdtemp()

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
    query_start_bound = int(time.time())

    # Pull out device names
    device_query = "select _id, NAME from DEVICE"
    res = cur.execute(device_query)
    for r in res.fetchall():
        devices[f"dev-{r[0]}"] = r[1]

    # Get SpO2 info
    spo2_data_query = ("SELECT TIMESTAMP, DEVICE_ID, TYPE_NUM, SPO2 FROM HUAMI_SPO2_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(spo2_data_query)
    for r in res.fetchall():
        row = {
                "timestamp": r[0] * 1000000000, # Convert to nanos
                fields : {
                    "spo2" : r[3]
                    },
                tags : {
                    "type_num" : r[2],
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)
    
    stress_data_query = ("SELECT TIMESTAMP, DEVICE_ID, TYPE_NUM, STRESS FROM HUAMI_STRESS_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(stress_data_query)
    for r in res.fetchall():
        row = {
                "timestamp": r[0] * 1000000000, # Convert to nanos
                fields : {
                    "stress" : r[3]
                    },
                tags : {
                    "type_num" : r[2],
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)    
    
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, RATE FROM HUAMI_SLEEP_RESPIRATORY_RATE_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        row = {
                "timestamp": r[0] * 1000000000, # Convert to nanos
                fields : {
                    "sleep_respiratory_rate" : r[2]
                    },
                tags : {
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)        


    data_query = ("SELECT TIMESTAMP, DEVICE_ID, PAI_LOW, PAI_MODERATE, PAI_HIGH, TIME_LOW," 
        "TIME_MODERATE, TIME_HIGH, PAI_TODAY, PAI_TOTAL "
        "FROM HUAMI_PAI_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        row = {
                "timestamp": r[0] * 1000000000, # Convert to nanos
                fields : {
                    "pai_low" : r[2],
                    "pai_moderate" : r[3],
                    "pai_high" : r[4],
                    "time_low" : r[5],
                    "time_moderate" : r[6],
                    "time_high" : r[7],
                    "pai_today" : r[8],
                    "pai_total" : r[9]
                    },
                tags : {
                    "device" : devices[f"dev-{r[1]}"]
                    }
            }
        results.append(row)        

    data_query = ("SELECT TIMESTAMP, DEVICE_ID, LEVEL, BATTERY_INDEX FROM BATTERY_LEVEL "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        row = {
                "timestamp": r[0] * 1000000000, # Convert to nanos
                fields : {
                    "battery_level" : r[2]
                    },
                tags : {
                    "device" : devices[f"dev-{r[1]}"],
                    "battery" : r[3]
                    }
            }
        results.append(row)        


    # Heart rates are spread across tables, depending on the sampling types
    rate_types = {
        "manual" : "HUAMI_HEART_RATE_MANUAL_SAMPLE",
        "max" : "HUAMI_HEART_RATE_MAX_SAMPLE",
        "resting" : "HUAMI_HEART_RATE_RESTING_SAMPLE"
        }
    
    for rate_type in rate_types:
        query = (f"SELECT TIMESTAMP, DEVICE_ID, HEART_RATE FROM {rate_types[rate_type]} "
            f"WHERE TIMESTAMP >= {query_start_bound} "
            "ORDER BY TIMESTAMP ASC")
        res = cur.execute(data_query)
        for r in res.fetchall():
            row = {
                    "timestamp": r[0] * 1000000000, # Convert to nanos
                    fields : {
                        "heart_rate" : r[2]
                        },
                    tags : {
                        "device" : devices[f"dev-{r[1]}"],
                        "sample_type" : rate_type
                        }
                }
            results.append(row)        
        



    return results

if not WEBDAV_URL:
    print("Error: WEBDAV_URL not set in environment")
    sys.exit(1)



webdav_options = {
    "webdav_hostname" : WEBDAV_URL,
    "webdav_login" : WEBDAV_USER,
    "webdav_password" : WEBDAV_PASS
    }

webdav_client = Client(webdav_options)
tempdir = fetch_database(webdav_client)

conn, cur  = open_database(tempdir)
# testing
res = cur.execute("SELECT name FROM sqlite_master")
print(res.fetchall())

extract_data(cur)

if tempdir not in ["/", ""]:
    print(tempdir)
    shutil.rmtree(tempdir)
    




