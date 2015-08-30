import boto
from dateutil import parser
import datetime
import jinja2

from jinja2 import FileSystemLoader
from jinja2.environment import Environment
from azure.storage import BlobService
import logging 

import os
if os.path.exists('.env'):
    for line in open('.env'):
        var = line.strip().split('=',1)
        if len(var) == 2:
            os.environ[var[0]] = var[1]


logger = logging.getLogger(__name__)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)



def get_link_text(name,date,my_dict):
    if "__all_current" in name:
        return "Snapshot of data on " + my_dict["Date of data download"]
    if "__diff" in name:
        return "New records observed in download on " + my_dict["Date of data download"]



def generate_website_and_upload_azure(azure_csv_container, azure_web_container):

    blob_service = BlobService(account_name=os.getenv('ACC_NAME'), account_key=os.getenv('ACCESS_KEY'))
    blob_list = blob_service.list_blobs(azure_csv_container)
    blob_name_list =  blob_list.blobs


    
    keys = []
    #Only keep files whose dates can be parsed
    for k in blob_name_list:
        try:
            parser.parse(k.name[:8])
            keys.append(k)
        except:
            pass

    keys = [k for k in keys if (".zip" in k.name or ".csv" in k.name)]



    
    my_array = []
    for k in keys:
        my_dict = {}
        url = r"http://fhrscsvs.blob.core.windows.net/{}/{}".format(azure_csv_container,k.name)
        name = k.name

        date = parser.parse(name[:8])
        dateformat = date.strftime("%a %d %b %Y")
        my_dict["Date of data download"] = dateformat

        my_dict["Size"] = sizeof_fmt(k.properties.content_length)

        name = get_link_text(name,dateformat,my_dict)
        
        my_dict["File"] = "<a href='{0}'>{1}</a>".format(url,name)
        
        my_array.append(my_dict)

    my_array = sorted(my_array, key=lambda k: k['File'], reverse=True) 

    table_array_fullsnapshot = [a for a in my_array if "__all_current" in a["File"]]
    table_array_differences = [a for a in my_array if "__diff" in a["File"]]

    template_dir = os.getenv('TEMPLATE_DIR')
    loader = jinja2.FileSystemLoader(template_dir)
    environment = jinja2.Environment(loader=loader)

    j_template = environment.get_template("template.html")

    order = ["File", "Size"]
        
    timestamp = datetime.datetime.now().strftime("%a %d %b %Y at %H:%M")
    
    import math
    sinarray = [(math.cos(math.radians(i*5-180))+1)*14 for i in range(0,73)]
    html = j_template.render(table_array_fullsnapshot=table_array_fullsnapshot, 
        order=order, 
        timestamp = timestamp, 
        sinarray=sinarray,
        table_array_differences=table_array_differences)


    blob_service.put_block_blob_from_text(
        azure_web_container,
        "index.html",
        html,
        x_ms_blob_content_type='text/html',
        text_encoding="utf-8",
    )

def upload_log():

    blob_service = BlobService(account_name=os.getenv('ACC_NAME'), account_key=os.getenv('ACCESS_KEY'))

    fpath = os.path.join(os.getenv('LOGS_DIR'),"log.log")

    blob_service.put_block_blob_from_path(
                    'log',
                    "log.log",
                    fpath,
                    x_ms_blob_content_type="text/plain"
                )

