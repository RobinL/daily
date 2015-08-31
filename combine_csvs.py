import os
import logging
logger = logging.getLogger(__name__)
import pandas as pd
import hashlib
import datetime

def combine_csvs(csvs_directory,output_directory):
    
    
    files = os.listdir(output_directory)
    files = [f for f in files if ".csv" in f]
    files = [os.path.join(output_directory,f) for f in files if "__" in f]
    
    for f in files:
        os.remove(f)

    #Get list of files to join

    file_list = os.listdir(csvs_directory)
    file_list = [f for f in file_list if "__" in f]
    file_list = [f for f in file_list if "FHRS" in f]
    file_list = [f for f in file_list if ".csv" in f]

    #Make name of output file
    file1 = file_list[0]
    file_list = [os.path.join(csvs_directory,d) for d in file_list if ".csv" in d]


    dt = datetime.datetime.now()

    if dt.time() < datetime.time(12):
        outfilename = file1[:8] + "__all_current_fhrs_am.csv"
    else:
        outfilename = file1[:8] + "__all_current_fhrs_pm.csv"

    #Write csv

    output_path = os.path.join(output_directory, outfilename)

    fout=open(output_path,"w")

    # first file:
    first_file = file_list.pop()
    for line in open(first_file):
        fout.write(line)
    # now the rest:    
    for this_file in file_list:
        f = open(this_file)
        f.next() # skip the header
        for line in f:
             fout.write(line)
        f.close() # not really needed
    fout.close()

    df = pd.read_csv(output_path)
    df["hash"] =df.apply(lambda x: hashlib.md5(",".join([str(y) for y in x])).hexdigest(),axis=1)
    df.to_csv(output_path, encoding="utf-8")
    return output_path

