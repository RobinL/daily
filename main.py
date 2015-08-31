
import os
import sys
import pandas as pd 
from upload_all_new import upload_all_new_azure
from download_csvs import download_csvs
from combine_csvs import combine_csvs
from my_zip import create_zip_from_file
from my_website import generate_website_and_upload_azure, upload_log

import logging
import sys
from create_diffs import create_difference



if os.path.exists('.env'):
    for line in open('.env'):
        var = line.strip().split('=',1)
        if len(var) == 2:
            os.environ[var[0]] = var[1]



logging.basicConfig(filename=os.path.join(os.getenv('LOGS_DIR'),"log.log"),
                            filemode='a',
                            level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',)



logger = logging.getLogger(__name__)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)

logger.info("----------------------------")
logger.info("starting script")
logger.info("current working directory is {}".format(os.getcwd()))
logger.info("python executable is {}".format(sys.executable))

azure_container = os.getenv('CONTAINER_NAME')
logger.info("uploading to azure container {}".format(azure_container))

# First we want to decide whether to run any code at all
# If there is a file uploaded on the website with a date in the past 7 days, then do nothing
# Else run all our code
logger.info("started - downloading csvs to {}".format(os.getenv('INDIVIDUAL_LA_CSV_DIR')))

download_csvs(os.getenv('INDIVIDUAL_LA_CSV_DIR'))

combined_csv_path = combine_csvs(os.getenv('INDIVIDUAL_LA_CSV_DIR'),os.getenv('COMBINED_LA_CSV_DIR'))

difference_csv_path = create_difference(combined_csv_path)

create_zip_from_file(combined_csv_path,os.getenv('FOR_UPLOAD_DIR'))



if difference_csv_path:
    logger.debug("creating difference")
    logger.debug(combined_csv_path)
    logger.debug(difference_csv_path)
    create_zip_from_file(difference_csv_path,os.getenv('FOR_UPLOAD_DIR'))


#Now we want a script that uploads any files in 'for_upload' to s3 which aren't already there
#Note that because the files are being transfered from EB to S3 this should be super fast - like 100MB/s
logger.info("uploading files to azure from {}".format(os.getenv('FOR_UPLOAD_DIR')))
upload_all_new_azure(os.getenv('FOR_UPLOAD_DIR'), azure_container, os.getenv('ACC_NAME'),os.getenv('ACCESS_KEY'))

# Finally we want to generate a little webpage to upload that contains all the links
logger.info("generating website {}".format(os.getenv('FOR_UPLOAD_DIR')))
generate_website_and_upload_azure(azure_container, "web")

logger.info("successfully completed whole script")
#Upload log
upload_log()



