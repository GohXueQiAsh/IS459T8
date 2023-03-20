# Install the Python ScrapingBee library:
# pip install scrapingbee

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
        print(x.get_attribute('data-ctorig'))
        link = x.get_attribute('data-ctorig')
        if link != None:
            links.append(link)
    count = count + 1
    print(count)
    if count <= 10:
        driver.find_element(
            By.XPATH, '//*[@id="___gcse_0"]/div/div/div/div[5]/div[2]/div/div/div[2]/div/div['+str(count)+']').click()

links = list(dict.fromkeys(links))
print(links)


for link in links:
    client = ScrapingBeeClient(
        api_key='6XXYR7NE3YAWYWZQXG8FPXC0PH019K6PGXEVFFV6MM144OPU3HEU3PJQZCLGCO0HJRQOON9XZXDI8V57')
    response = client.get(link,
                          params={
                              'wait_for': '.bbWrapper',
                              'extract_rules': {
                                  "Comment": {
                                      "selector": ".bbWrapper",
                                      "type": "list"
                                  },
                                  "Date": {
                                      "selector": "time[itemprop='datePublished']",
                                      "type": "list"
                                  },
                                  "User_name": {
                                      "selector": ".message-name > a",
                                      "type": "list"
                                  }
                              },
                              'timeout': '40000'
                          }
                          )
    content = json.loads(response.content)
    contentDUMP = json.dumps(content)

    #content1 = b'{"comment": ["Bivalent COVID-19 vaccination for 18-49 age group to be rolled out from Nov 7 SINGAPORE: The bivalent Moderna/Spikevax COVID-19 vaccine will be offered to people aged 18 to 49 from Nov 7, said the Ministry of Health (MOH) on Friday (Nov 4). Eligible individua www.channelnewsasia.com", "yayyy can get free mc again Sent from EDMWER app!", "standby wait for sms", "If don\'t take this one, would we be considered as unvaccinated?", "Eligible individuals will receive an SMS with a personalised booking link to make an appointment at one of the ministry\'s Joint Testing and Vaccination Centres (JTVCs), to get an additional dose of the vaccine. \\"In particular, those who have yet to complete their minimum protection series should do so as soon as possible,\\" said MOH. This means that after achieving minimum protection, these individuals should receive an additional booster dose - recommended to be the updated bivalent vaccine - between five months to one year from their last jab. \\"Individuals will be considered up to date with their COVID-19 vaccination if they have received at least the minimum protection and their last vaccine dose was received within the past one year,\\" said MOH.", "\\"Individuals will be considered up to date with their COVID-19 vaccination if they have received at least the minimum protection and their last vaccine dose was received within the past one year,\\" said MOH. wah this statement... means soon will see vaccination status changed to unvaccinated?", "I thought Endemic? Why still need meh meh pak?", "jab ki lan pui!!! Sent from EDMWERot be taking", "elimmel said: \\"Individuals will be considered up to date with their COVID-19 vaccination if they have received at least the minimum protection and their last vaccine dose was received within the past one year,\\" said MOH. wah this statement... means soon will see vaccination status changed to unvaccinated? Click to expand... Means simi Exceed one yr n redo all again?", "Gotta collect em all!", "treeskull said: Alrdy XBB variant still poking BA1 and 2 Click to expand... Variants galore. XBB.4 (right most in diagram) reach Level9 Liao. \xe6\xad\xa6\xe5\x8a\x9f\xe7\x9b\x96\xe4\xb8\x96", "Sent from EDMWER app!", "Irenicis said: If don\'t take this one, would we be considered as unvaccinated? Click to expand... This means officially no count your last 5 shots as it moves to the new \\"updated\\" system. Every shot from now on only valid one year or less . Keep chasing and pray dont one road good walk at young age can le.", "Govt should pay people to take the vaccine then higher rate of uptake", "elimmel said: \\"Individuals will be considered up to date with their COVID-19 vaccination if they have received at least the minimum protection and their last vaccine dose was received within the past one year,\\" said MOH. wah this statement... means soon will see vaccination status changed to unvaccinated? Click to expand... Now also don\'t have restriction for unvaccinated, why would sinkies take the booster"], "date": ["Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022", "Nov 4, 2022"], "user_name": ["yperic", "1234pm", "DeadmanINC", "Irenicis", "yperic", "elimmel", "starry_starry_night", "articland05", "kuti-kuti", "popsune1", "treeskull", "Bardiel", "mrclubbie", "Laneige", "bombshell", "tmkedmw", "Taro Pie", "Smtsai", "Visor9999", "Visor9999"]}'
    #content1json = json.loads(content1)

    print('COMMENT COUNT:' + str(len(content['Comment'])))
    print('DATE COUNT:' + str(len(content['Date'])))
    print('USER COUNT:' + str(len(content['User_name'])))

    df = pd.read_json(contentDUMP)
    dfs.append(df)


dfConcat = pd.concat(dfs, ignore_index=True)
check_file = os.path.isfile('scraping_HWZ.csv')
if check_file:
    dfConcat.to_csv('scraping_HWZ.csv', index=False, mode='w')
else:
    dfConcat.to_csv('scraping_HWZ.csv', index=False)
