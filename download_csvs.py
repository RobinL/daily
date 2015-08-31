import pandas as pd  #this is a data analysis package
import requests  #this is a package that allows us to send off web requests
from bs4 import BeautifulSoup #This allows us to parse xml files.  There are better alternatives but I'm familiar with this one
import sys
import re #import regular expressions, text manipulation language
import os
import shutil
import logging
import gc
from my_website import upload_log

logger = logging.getLogger(__name__)


def download_csvs(csv_file_directory):

    # shutil.rmtree(csv_file_directory)
    # os.makedirs(csv_file_directory)

    files = os.listdir(csv_file_directory)
    files = [f for f in files if ".csv" in f]
    files = [os.path.join(csv_file_directory,f) for f in files if "__" in f]
    
    for f in files:
        os.remove(f)


    url = 'http://ratings.food.gov.uk/open-data/en-GB'

    #Try to download the page at most 10 times.

    try:
        r  = requests.get(url) 
    except Exception as e:
        logger.error(e.message[1])
        sys.exit()
       
    data = r.text  #Get html from above url - this is a list of all the xml links
    soup = BeautifulSoup(data)  #parse into dictionary-like structure to extract data


    #Get a list of all of the hyperlinks of the page that are in English and contain FHRS data.  Note re.compile is basically doing a search/filter on the links 
    all_links  = soup.find_all("a",text = re.compile('English'),href = re.compile('FHRS'))
    del r
    del data
    del soup
    logger.debug(str(len(all_links)) + " links were found")

    #Format:
    links = [l["href"] for l in all_links]

    if len(links)< 350:
        logging.error("fewer than 350 xml files were found, there was some error")
        sys.exit()

    #a now contains a list of all the hyperlinks of xml we want to visit and download
    # links = [link for link in links if "702" in link]
    links_to_do = set(links[:52])


    #this is a list of fields that we want in our final table of data
    fieldslist = ["FHRSID",
    "LocalAuthorityBusinessID",
    "BusinessName",
    "BusinessType",
    "BusinessTypeID",
    "RatingValue",
    "RatingKey",
    "RatingDate",
    "LocalAuthorityCode",
    "LocalAuthorityName",
    "LocalAuthorityWebSite",
    "LocalAuthorityEmailAddress",
    "Hygiene",
    "Structural",
    "ConfidenceInManagement",
    "SchemeType",
    "Longitude",
    "Latitude",
    "AddressLine1",
    "AddressLine2",
    "AddressLine3",
    "PostCode",
    "AddressLine4",
    "RightToReply"
    ]

    #convert to lowercase
    fieldslist = [x.lower() for x in fieldslist]


    #finalarr is an array which will contain a list of each row we want in the final dataset
    import datetime
    date_string = datetime.date.today().strftime("%Y%m%d")

    #counter is just so we can keep track of progress, it isn't needed
    counter_for_done = 0
    counter_for_error = 0

    

    all_links_len = len(links_to_do)

    failed_count_dict = {link:0 for link in links_to_do}
    


    while len(links_to_do)>0:
        if counter_for_done % 10 ==0:
            logger.debug("completed " + str(counter_for_done) + " xml downloads")
            upload_log()

        
        if counter_for_error > all_links_len/3:
            logger.error("Even after retrying the downloads, we were unable to download all the links.  Exiting")
            sys.exit()
        
        
        this_link = links_to_do.pop()

     
        #download data
        try:
            r = requests.get(this_link)
        except Exception as e:
            logger.error(e.message[1])
            sys.exit()
            
        if "Internal Server Error" in r.text:
            links_to_do.add(this_link)
            logger.debug("Internal server error on link: " + this_link)
            continue
            
      
        
        #parse data
        try:
            soup = BeautifulSoup(r.text)
            del r
        except:
            #If this goes wrong put link back into pile
            links_to_do.add(this_link)
            logger.debug("Can't convert to soup on link: " + this_link)
            continue
        

        
        #find list of establishments
        try:
            est = soup.find_all("establishmentdetail")
        except:
            links_to_do.add(this_link)
            logger.debug("Can't find establishmentdetail in link: " + this_link)
            continue
        
        #
        if len(est) <1:
            failed_count_dict[this_link] +=1
            if failed_count_dict[this_link] > 3:
                #Give up on this one
                counter_for_error +=1
                logger.debug("Can't find any establishmentdetails in link even after 3 attempts: " + this_link)
                continue
            else:
                #Try again
                links_to_do.add(this_link)
                continue

        
            
        #for each establishment, find the data in each field and add to dictionary
        finalarr = []
        for i in est:
            this_dict = {}
            for j in fieldslist:
                te = None

                try:
                    te = i.find(j).text
                except:
                    pass
                this_dict[j] = te
            finalarr.append(this_dict)  #add dictionary to array
        
        #Check that the csv looks ok:
        
        df = pd.DataFrame(finalarr)
        
        #Does it have more than one row?
        if df.shape[0] < 1:
            links_to_do.add(this_link)
            logger.debug("Can't find any premesis in link: " + this_link)
            continue
        
        #Now write this to csv file 
        
        
        file_name = this_link.replace(r"http://ratings.food.gov.uk/OpenDataFiles/","").replace("en-GB.xml","")
        file_name = os.path.join(csv_file_directory, date_string+"__"+file_name+".csv")
        df.to_csv(file_name, encoding="utf-8", index=False)
        counter_for_done +=1

        del df
        del finalarr
        gc.collect()


    for i in failed_count_dict:
        if failed_count_dict[i]>3:
            logger.warning("the file " + i + " contained no establishments")
    logger.info("completed successfully")

