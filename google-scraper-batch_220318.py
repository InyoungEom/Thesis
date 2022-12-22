import pandas as pd
import argparse

# for scraping app info and reviews from Google Play
from google_play_scraper import app, Sort, reviews

# for pretty printing data structures
from pprint import pprint

# for keeping track of timing
import datetime as dt
from tzlocal import get_localzone

# for building in wait times
import random
import time
from tqdm import tqdm


# get parameters
def Get_arguments():
    parser = argparse.ArgumentParser()
    
    # language
    parser.add_argument("--lang", type=str, default="en")
    # country
    parser.add_argument("--country", type=str, default="us")
    # batch size
    parser.add_argument("--batch", type=int, default=600)  
    # data path
    parser.add_argument("--data", type=str, default="data/app_ids.csv")
    
    
    return parser.parse_args()


# main
def main(args):
    ## Read in file containing app names and IDs
    app_df = pd.read_csv(args.data)

    ## Get list of app names and app IDs
    app_names = list(app_df['app_name'])
    app_ids = list(app_df['android_appID'])
    
    lang = args.lang
    country = args.country
    
    # Number of reviews to scrape per batch
    count = 100
       
    for app_name, app in zip(app_names, app_ids):      
        # Empty list for storing reviews, and initialize after first loop
        app_reviews = []
        
        # To keep track of how many batches have been completed
        batch_num = 0 
        
        # print app name when start scrapping reviews
        print(f'start scrapping {app_name} reviews!')  
               
        # Retrieve reviews (and continuation_token) with reviews function
        rvs, token = reviews(
            app, 
            lang=lang,
            country=country,
            sort=Sort.NEWEST,
            count=count,
            filter_score_with=None
        )
        
        # For each review obtained
        for r in rvs:
            r['app_name'] = app_name # add key for app's name
            r['app_id'] = app
        
        # Add the list of review dicts to overall list
        app_reviews.extend(rvs)
        
        # Increase batch count by one
        batch_num +=1 
        print(f'Batch {batch_num} completed.')
        
        # Wait 1 to 5 seconds to start next batch
        time.sleep(random.randint(1,5))
        
        # Append review IDs to list prior to starting next batch
        pre_review_ids = []
        for rvw in app_reviews:
            pre_review_ids.append(rvw['reviewId'])
            
        # Loop through at most max number of batches
        for batch in range(args.batch-1):
            rvs, token = reviews( # store continuation_token
                app,
                lang=lang,
                country=country,
                sort=Sort.NEWEST,
                count=count,
                # using token obtained from previous batch
                continuation_token=token
            )
            
            # Append unique review IDs from current batch to new list
            new_review_ids = []
            for r in rvs:
                new_review_ids.append(r['reviewId'])
                
                # And add keys for name and id to ea review dict
                r['app_name'] = app_name # add key for app's name
                r['app_id'] = app        # add key for app's id
            
            
            # Add the list of review dicts to main app_reviews list
            app_reviews.extend(rvs)
            
            # Increase batch count by one
            batch_num +=1
            
            # Break loop and stop scraping for current app if most recent batch
            # did not add any unique reviews
            all_review_ids = pre_review_ids + new_review_ids
            if len(set(pre_review_ids)) == len(set(all_review_ids)):
                print(f'No reviews left to scrape. Completed {batch_num} batches.\n')
                break
            
            # all_review_ids becomes pre_review_ids to check against 
            # for next batch
            pre_review_ids = all_review_ids
            
            if batch_num%10==0:
                # print update on number of batches
                print(f'Batch {batch_num} completed.')
        
        app_review_df = pd.DataFrame(app_reviews)
        app_review_df.to_csv('./scrap_data/reviews_{}_{}_{}.csv'.format(
            app_name, country, args.batch*100), index=None, header=True)
        
        print("# # # # # # # # # # # # # # # # # # # # # #")

if __name__ == '__main__':
    args = Get_arguments()
    main(args)
