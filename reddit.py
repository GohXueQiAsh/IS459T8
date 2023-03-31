from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import time
import re
from urllib.request import urlopen
import json
from selenium.webdriver.common.keys import Keys
import praw
import pandas as pd
import datetime as dt
from webdriver_manager.chrome import ChromeDriverManager
import boto3
from io import BytesIO
import schedule
import datetime


# def download_and_upload_comments():
#     today = datetime.datetime.today()
#     if today.day == 31:

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

def scrape_url(source_path):
    page = 1
    url = []
    search_list = ['singapore','bivalent']
    while(page <= 2): 
        browser = webdriver.Chrome('chromedriver',options=chrome_options)
        browser.get(source_path)
        Pagelength = browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        source = browser.page_source
        soup = bs(source, 'html.parser')
        body = soup.find('body')

        threads = soup.find_all("header", class_="search-result-header")
        next_page = soup.find_all("span", class_="nextprev")
        source_path = next_page[0].find("a").get('href')
                    
        for link in threads:
            title = link.find("a").text
            if re.compile('(?=.*' + ')(?=.*'.join(search_list) + ')', re.IGNORECASE).search(title): #re.IGNORECASE is used to ignore case
                url_string = link.find('a').get('href')
                url.append(url_string)
        page += 1            
    return url

source_path = "https://old.reddit.com/search?q=singapore+bivalent"
url_list = scrape_url(source_path)

reddit = praw.Reddit(client_id='', \
                    client_secret='', \
                    user_agent='', \
                    username='', \
                    password='')

topics_dict = { "author": [],
                "submission" : [],
                "body": [],
                "score": [],
                "id": [],
                "parent_id": [],
                "created": []}

def get_date(created):
    return dt.datetime.fromtimestamp(created)

def process_topic_data(submission):
    comments = []
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        timestamp = get_date(comment.created)
        topics_dict["author"].append(comment.author)
        topics_dict["submission"].append(comment.submission)
        topics_dict["body"].append(comment.body)
        topics_dict["score"].append(comment.score)
        topics_dict["id"].append(comment.id)
        topics_dict["parent_id"].append(comment.parent_id)
        topics_dict["created"].append(timestamp)
    return comments

for link in url_list:
    submission = reddit.submission(url=link)
    process_topic_data(submission)

topics_data = pd.DataFrame(topics_dict)

topics_data = topics_data.dropna()

csv_buffer = topics_data.to_csv(index=False).encode()
csv_file = BytesIO(csv_buffer)

s3 = boto3.resource('s3')
bucket_name = 'is459-g1t8-project'  # replace this with your S3 bucket name
object_key = 'input/reddit.csv'  # the key under which the object will be stored in the S3 bucket
s3.Bucket(bucket_name).upload_fileobj(csv_file, object_key)

# # schedule the job to run once every day at a specific time
# schedule.every().day.at(':00').do(download_and_upload_comments)

# # run the scheduled job
# while True:
#     schedule.run_pending()
#     time.sleep(1)