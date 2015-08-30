import os 
from dateutil import parser
import datetime
import pandas as pd

import logging

logger = logging.getLogger(__name__)

def create_difference(new_csv_path):

    print "new_csv_path {}".format(new_csv_path)
    filename = os.path.basename(new_csv_path)
    print "filename {}".format(filename)

    date = parser.parse(filename[:8]).date()

    csvs = os.listdir(os.path.dirname(new_csv_path))
    csvs = [csv for csv in csvs if "all_current" in csv]
    csvs = [{"file":this_file, "date":parser.parse(this_file[:8]).date()} for this_file in csvs]

    newlist = sorted(csvs, key=lambda k: k['date'], reverse=True) 

    older_dates = [d for d in newlist if d["date"]<date]

    if len(older_dates)>0:
        most_recent_previous_file = older_dates[0]["file"]
    else:
        return None
        
    most_recent_previous_file_full  = os.path.join(os.path.dirname(new_csv_path),most_recent_previous_file)
    #Now we just find the diff

    new = pd.read_csv(new_csv_path)

    old = pd.read_csv(most_recent_previous_file_full,usecols=["hash"])

    diff = new[-new["hash"].isin(old["hash"].unique())]

    diff_file_path = os.path.join(os.path.dirname(new_csv_path),filename[:8] + "__diff_from_" + most_recent_previous_file[:8]+".csv")
    diff.to_csv(diff_file_path, index=False)
    return diff_file_path 
