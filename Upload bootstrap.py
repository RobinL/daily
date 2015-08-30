
# coding: utf-8

# In[3]:

import os

from azure.storage import BlobService


if os.path.exists('.env'):
    for line in open('.env'):
        var = line.strip().split('=',1)
        if len(var) == 2:
            os.environ[var[0]] = var[1]
            
            


# In[4]:

blob_service = BlobService(account_name=os.getenv('ACC_NAME'), account_key=os.getenv('ACCESS_KEY'))


# In[32]:

b_path = r"C:\Users\Robin\Downloads\bootstrap-3.3.5-dist\bootstrap-3.3.5-dist"


# In[12]:

for root, subdirs, files in os.walk(b_path):
     with open(os.path.join(subdirs,files)) as file:
                blob_service.put_block_blob_from_file(
                    'csvs',
                    subdirs+files,
                    file,
                    x_ms_blob_content_type='zip'
                )


# In[34]:

for path, subdirs, files in os.walk(b_path):
    for name in files:
        full_path = os.path.join(path, name)
        
        path_no_file = os.path.split(full_path)[0]
        
        last_dir = os.path.split(path_no_file)[1]
        
        print last_dir+"/"+name
        
        with open(full_path) as file:
                blob_service.put_block_blob_from_file(
                    'web',
                    last_dir+"/"+name,
                    file,
                    x_ms_blob_content_type='text/'+last_dir
                    
                )
        
       


# In[24]:

for path, subdirs, files in os.walk(root):
    for name in files:
        print os.path.join(path, name)


# In[ ]:



