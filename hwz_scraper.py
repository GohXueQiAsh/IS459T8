import json
import pandas as pd
import os.path
from scrapingbee import ScrapingBeeClient
from bs4 import BeautifulSoup
import time
from urllib.request import Request, urlopen
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString, Tag
import re
import pandas as pd
import numpy as np
import boto3
from io import BytesIO


dfs = []
links = []
count = 1

options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging'])

driver = webdriver.Chrome(service=Service(
    executable_path=ChromeDriverManager().install()), options=options)
driver.get('https://www.hardwarezone.com.sg/search/forum/Bivalent')

while count <= 10:
    time.sleep(2)
    resultBoxElement = driver.find_element(
        By.CLASS_NAME, 'gsc-resultsbox-visible')
    URLtitle = resultBoxElement.find_elements(By.CLASS_NAME, 'gs-title')
    for x in URLtitle:
        link = x.get_attribute('data-ctorig')
        if link != None:
            links.append(link)
    count = count + 1
    if count <= 10:
        driver.find_element(
            By.XPATH, '//*[@id="___gcse_0"]/div/div/div/div[5]/div[2]/div/div/div[2]/div/div['+str(count)+']').click()
        
links = list(dict.fromkeys(links))

def scraper(discussion, url, firstPage):
    #extract body text
    comments = discussion.find(class_="message-body js-selectToQuote")
    s = comments.findAll("br")
    threads = discussion.find_all(class_="bbWrapper")
    for posts in threads:
        text = ""
        if posts.find("br") == None:
            text = posts.text

        else:
            stringified = str(posts)
            if ('class="bbCodeBlock-sourceJump"' in stringified):

                if(stringified[23:34] != "<blockquote"):

                    start = stringified.find('bbWrapper">')
                    end = stringified.find('<br/>')
                    text = stringified[start + 11:end]
            for br in posts:
                next_s = br.nextSibling
                if not (next_s and isinstance(next_s,NavigableString)):
                    continue
                next2_s = next_s.nextSibling
                if next2_s and isinstance(next2_s,Tag) and next2_s.name == 'br':
                    text += str(next_s).strip() + " "
                else:
                    text += str(next_s).strip()

        if("Click to expand..." in text):
            text = str(text)[str(text).find("Click to expand...") + 18:]
        text = text.strip()
        data_dictionary["body"].append(text)

    #extract author and thread 
    usernames = discussion.find_all(class_="message-name")
    for username in usernames:
        if username.string[0] != "@":
            if(username.text[0] != " "):
                data_dictionary["author"].append(username.string)

    #extract reaction score
    profiles = discussion.find_all(class_="message-userExtras")
    for profile in profiles:
        s = profile.find_all(class_={"pairs pairs--justified"})
        data_dictionary["score"].append(s[2].find("dd").text)
        data_dictionary["source"].append("hwz")

    #extract the dates
    date_body = discussion.find_all(class_="message-attribution-main listInline")
    date = 0
    for date_message in date_body:
        extracted_date = date_message.find(class_="u-dt")
        if extracted_date == None:
            data_dictionary["date"].append(date)
        else:
            data_dictionary["date"].append(extracted_date.text)
            
    
    
    return

data_dictionary = { "author" : [], "body" : [], "score" : [], "date" : [], "source":[]}

def scrape_threads(list):
    for url in list:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        count = 0
        firstPage = True
        if(soup.find(class_="pageNav-jump pageNav-jump--next") == None):
            if (soup.find("div",class_ = "block-body js-replyNewMessageContainer") != None):
                discussion = soup.find("div",class_ = "block-body js-replyNewMessageContainer")
                scraper(discussion, url, firstPage)

        while((soup.find(class_="pageNav-jump pageNav-jump--next")) != None):
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            discussion = soup.find("div",class_ = "block-body js-replyNewMessageContainer")            
            scraper(discussion, url, firstPage)
            firstPage = False
            
            #next page
            if(soup.find(class_="pageNav-jump pageNav-jump--next") != None):
                url = "https://forums.hardwarezone.com.sg/" + soup.find(class_="pageNav-jump pageNav-jump--next")["href"]
            count += 1
                    
    return data_dictionary

hwz_dict = scrape_threads(links)

hwz_df = pd.DataFrame.from_dict(hwz_dict)
hwz_df['body'] = hwz_df['body'].replace('', np.nan)
hwz_df = hwz_df.dropna()

csv_buffer = hwz_df.to_csv(index=False).encode()
csv_file = BytesIO(csv_buffer)

s3 = boto3.resource('s3')
bucket_name = 'is459-g1t8-project'  # replace this with your S3 bucket name
object_key = 'input/hwz.csv'  # the key under which the object will be stored in the S3 bucket
s3.Bucket(bucket_name).upload_fileobj(csv_file, object_key)