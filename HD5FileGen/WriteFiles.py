import os
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
import h5py
import time
import json
import random



def writeFile(service_client,count):
    file_name = 'test_{}.hdf5'.format(count)
    f = h5py.File(file_name, 'w')
    f['mydataset'] = get_temps()
  
    print(file_name)
    f.close()
    upload_file_to_directory(service_client,file_name)


def get_temps()->[]:
    return(random.sample(range(30, 110), 50))


def upload_file_to_directory(service_client,file_name):
    try:

        file_system_client = service_client.get_file_system_client(file_system="testresults")

        directory_client = file_system_client.get_directory_client("ingest")
        
        file_client = directory_client.create_file(file_name)
        local_file = open(file_name,'rb')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    except Exception as e:
      print(e)



def main():

    count = 0
    number_of_writes = 3
    wait = 5
   
    with open('config.json','r') as f:
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




