import os
import sys

import logging

logger = logging.getLogger(__name__)

from azure.storage import BlobService

def upload_all_new_azure(local_folder, azure_container, account_name, account_key):



    blob_service = BlobService(account_name=os.getenv('ACC_NAME'), account_key=os.getenv('ACCESS_KEY'))

    blob_list = blob_service.list_blobs(azure_container)

    blob_name_list = [b.name for b in blob_list.blobs]

    blob_name_set = set(blob_name_list)

    #Now for each file in local forlder see whether it's in the s3folder
    
    localfiles = os.listdir(local_folder)
    localfiles = [f for f in localfiles if "~" not in f]
    localfiles = [f for f in localfiles if f[0] != "."]
    localfiles = [f for f in localfiles if (".zip" in f or ".csv" in f)]
    
    localfiles = set(localfiles)

    
    files_to_upload = localfiles - blob_name_set



    orig_len =len(files_to_upload) 
    error_counter = 0
    while len(files_to_upload)>0:
        if error_counter>orig_len:
            logger.error("too many upload failures, exiting")
            sys.exit()
        filename = files_to_upload.pop()

        try:
            blob_service.put_block_blob_from_path(
                'csvs',
                filename,
                os.path.join(local_folder,filename)
            )

        except Exception:
            error_counter +=1
            logging.error(filename + " failed to upload")
            files_to_upload.add(filename)






