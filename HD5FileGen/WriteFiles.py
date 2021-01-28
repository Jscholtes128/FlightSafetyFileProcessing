
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

 ## ##########################################
 # Run WrtieFiles.py from HD5FilesGen Directory 
 #############################################

import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
import h5py
import time
import json
import random


## Create HDF5 file and upload to Azure Data Load Store
def writeFile(service_client,file_identifier):
    file_name = 'test_{}.hdf5'.format(file_identifier)
    f = h5py.File(file_name, 'w')
    f['mydataset'] = get_temps() ## Populate with simple test data, list of random int
  
    print(file_name)
    f.close()

    # Upload data to Azure Data Lake Store
    upload_file_to_directory(service_client,file_name)

## Generate sample data, int list
def get_temps()->[]:
    return(random.sample(range(30, 110), 50))

## Write file using file_name to Azure Data Lake Store using provided file name
def upload_file_to_directory(service_client,file_name):
    try:

        # ADLS Container (this is currently hard-coded for the test) 
        file_system_client = service_client.get_file_system_client(file_system="testresults")

        # ADLS Directory path (this is currently hard-coded for the test) 
        directory_client = file_system_client.get_directory_client("ingest")
        
        # Create file on ADLS
        file_client = directory_client.create_file(file_name)

         ## Read load test file
        local_file = open(file_name,'rb')
        file_contents = local_file.read()

        #Write test HDF5 file to ADLS
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    except Exception as e:
      print(e)


## This application will generate test HDF5 files to write to Azure Data Lake Store
# when the Azure Function is deployed and running - the blob create will trigger the function
# and write the processed files into the output directory
def main():

    count = 0 ## track iteration
    number_of_writes = 4  ## number of test files to write
    wait = 2 ## seconds to wait between writes
   
  
    with open('config.json','r') as f:
        #storage_account_name and storage_account_key are provided in the 'config.json' for local debug and as 
        # function app configs for deployment
        config = json.load(f)
        storage_account_name = config['storage_account_name']
        storage_account_key = config['storage_account_key']

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    while count < number_of_writes:
        writeFile(service_client,count)
        time.sleep(wait)
        count+=1



if __name__ == "__main__":
    main()




