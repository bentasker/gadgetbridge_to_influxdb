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
import sys
import tempfile
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

if tempdir not in ["/", ""]:
    print(tempdir)
    shutil.rmtree(tempdir)
    




