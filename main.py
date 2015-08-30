#!/usr/bin/env python

from download_csvs import download_csvs
from combine_csvs import combine_csvs
from my_zip import create_zip_from_file
from upload_all_new import upload_all_new_azure
from my_website import generate_website_and_upload_azure, upload_log
# from check_last_update import less_than_a_week_since_last_upload
import logging
import sys
from create_diffs import create_difference

import os
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

logger.info("starting script")

azure_container = os.getenv('CONTAINER_NAME')

# First we want to decide whether to run any code at all
# If there is a file uploaded on the website with a date in the past 7 days, then do nothing
# Else run all our code



logger.info("started - downloading csvs")

download_csvs(os.getenv('INDIVIDUAL_LA_CSV_DIR'))

combined_csv_path = combine_csvs(os.getenv('INDIVIDUAL_LA_CSV_DIR'),os.getenv('COMBINED_LA_CSV_DIR'))

difference_csv_path = create_difference(combined_csv_path)

create_zip_from_file(combined_csv_path,os.getenv('FOR_UPLOAD_DIR'))

print "combined_csv_path {}".format(combined_csv_path)
print "difference_csv_path {}".format(difference_csv_path)

if difference_csv_path:
    logger.debug("creating difference")
    logger.debug(combined_csv_path)
    logger.debug(difference_csv_path)
    create_zip_from_file(difference_csv_path,os.getenv('FOR_UPLOAD_DIR'))


#Now we want a script that uploads any files in 'for_upload' to s3 which aren't already there
#Note that because the files are being transfered from EB to S3 this should be super fast - like 100MB/s
upload_all_new_azure(os.getenv('FOR_UPLOAD_DIR'), azure_container, os.getenv('ACC_NAME'),os.getenv('ACCESS_KEY'))

# Finally we want to generate a little webpage to upload that contains all the links

generate_website_and_upload_azure("csvs", "web")

logger.info("successfully completed whole script")
#Upload log
upload_log()



