import zipfile
import zlib
import os 
import logging
logger = logging.getLogger(__name__)
import datetime

def create_zip_from_file(input_path, output_path):
    file_name = os.path.basename(input_path)
    output_path = os.path.join(output_path, file_name)
    
    output_path = output_path.replace(".csv", ".zip")
    compression = zipfile.ZIP_DEFLATED

    modes = { zipfile.ZIP_DEFLATED: 'deflated',
          zipfile.ZIP_STORED:   'stored',
          }

    zf = zipfile.ZipFile(output_path, mode='w')
    
    try:
        zf.write(input_path,os.path.basename(input_path), compress_type=compression,)
    finally:
        zf.close()


