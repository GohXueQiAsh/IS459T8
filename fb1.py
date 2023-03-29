"""
Download comments for a public Facebook post.
"""
import facebook_scraper as fs
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
#driver.get('https://www.hardwarezone.com.sg/search/forum/Bivalent')
# Initialize the Chrome webdriver using WebDriver Manager
#driver = webdriver.Chrome(ChromeDriverManager().install())
driver = webdriver.Chrome('chromedriver',options=chrome_options)


# Navigate to the Facebook login page
driver.get('https://www.facebook.com/')

# Find the email and password fields and enter your login credentials
email = driver.find_element(
        By.NAME, 'email')
email.send_keys('is459g1t8@gmail.com')
password = driver.find_element(
        By.NAME, 'pass')
password.send_keys('qwer1234!')

# Press the Enter key to submit the login form
password.send_keys(Keys.ENTER)

# Wait for the login to complete
time.sleep(5)

# get POST_ID from the URL of the post which can have the following structure:
# https://www.facebook.com/USER/posts/POST_ID
# https://www.facebook.com/groups/GROUP_ID/posts/POST_ID
# https://www.facebook.com/sghealthministry/posts/take-your-booster-as-soon-as-you-are-eligible-here-are-some-answers-to-guide-you/317010660454364/
POST_ID = "317010660454364"

# number of comments to download -- set this to True to download all comments
MAX_COMMENTS = 200

# get the post (this gives a generator)
gen = fs.get_posts(
    post_urls=[POST_ID],
    options={"comments": MAX_COMMENTS, "progress": True}
)

# take 1st element of the generator which is the post we requested
post = next(gen)

# extract the comments part
comments = post['comments_full']

# process comments as you want...

myFile = open('firstcovidnews.csv', 'w', encoding='utf-8')
writer = csv.writer(myFile)
writer.writerow(['comment_id','comment_url',	'commenter_id',	'commenter_url','commenter_name' ,'commenter_meta','comment_text','comment_time',	'comment_image','comment_reactors',	'comment_reactions','comment_reaction_count','replies'])
for dictionary in comments:
    writer.writerow(dictionary.values())
myFile.close()
myFile = open('firstcovidnews.csv', 'r')
myFile.close()
    

#     # e.g. ...get the replies for them
#     for reply in comment['replies']:
#         print(' ', reply)