############################################################################################
#Permission is hereby granted, free of charge, to any person obtaining a copy of
#this software and associated documentation files (the "Software"), to deal in
#the Software without restriction, including without limitation the rights to
#use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
#of the Software, and to permit persons to whom the Software is furnished to do
#so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
###########################################################################################

import logging
import os
import azure.functions as func
import h5py
from io import BytesIO, TextIOBase
from azure.storage.filedatalake import DataLakeServiceClient
import json


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    ## Read Stream Bytes in as HDF5 file
    hd5_file = h5py.File(BytesIO(myblob.read()),'r')

    ## For the test, the HD5 files contain a simple numeric list. To save to a text file 
    # this list hd5 ndarry is converted to a string list then joined with the newline delimeter '\n'
    data =  '\n'.join(hd5_file['mydataset'][:].astype(str).tolist())
  
    logging.info(data)

    ## Save contents of hd5 to Azure Data Lake Storage as simple text (.txt)
    # the hd5 extention is removed and the '.txt' extention is hard-coded on for this test
    upload_file_to_directory(data,strip_extension(myblob.name.split('/')[-1])+'.txt')


## Write files contents to Azure Data Lake Store using provided file name
def upload_file_to_directory(file_contents,file_name):
    try:

        #storage_account_name and storage_account_key are provided in the 'local.settings.json' for local debug and as 
        # function app configs for deployment
        storage_account_name = os.environ["storage_account_name"]
        storage_account_key = os.environ["storage_account_key"]

        #Create instance of Data Lake Service Client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=storage_account_key)

        # ADLS Container (this is currently hard-coded for the test) 
        file_system_client = service_client.get_file_system_client(file_system="testresults")

        # ADLS Directory path (this is currently hard-coded for the test) 
        directory_client = file_system_client.get_directory_client("triggeroutput")
        
        # This will get existing file or create new
        file_client = directory_client.get_file_client(file_name)
 
        # Upload file contents
        file_client.upload_data(file_contents, overwrite=True)

    except Exception as e:
      print(e)


# Preserve file name - strip extension 
def strip_extension(file_name):
    return ''.join(file_name.split('.')[:-1])



