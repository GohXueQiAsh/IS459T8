import json
import pandas as pd
import os.path
from scrapingbee import ScrapingBeeClient
from bs4 import BeautifulSoup
import time
import schedule
import datetime as dt
from urllib.request import Request, urlopen

import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString, Tag
import re
import pandas as pd
import numpy as np
import boto3
from io import BytesIO

# def download_and_upload_comments():
#     today = datetime.datetime.today()
#     if today.day == 31:
dfs = []
count = 1
links = ['https://forums.hardwarezone.com.sg/threads/bivalent-covid-19-vaccination-for-18-49-age-group-to-be-rolled-out-from-nov-7.6831404/', 'https://forums.hardwarezone.com.sg/threads/moh-will-invite-people-aged-18-to-49-for-bivalent-vaccination-later-in-2022.6825082/', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/', 'https://forums.hardwarezone.com.sg/threads/modernas-bivalent-covid-19-booster-vaccine-in-spore-by-end-sept-pfizers-being-evaluated.6809768/', 'https://forums.hardwarezone.com.sg/threads/u-s-says-pfizers-bivalent-covid-shot-may-be-linked-to-stroke-in-older-adults.6861221/', 'https://forums.hardwarezone.com.sg/threads/omg-us-sees-early-signal-of-pfizer-bivalent-covid-19-shots-link-to-stroke.6861239/page-4', 'https://forums.hardwarezone.com.sg/threads/oyk-moderna-spikevax-bivalent-vaccine-same-safety-profile-as-original-dose.6820475/', 'https://forums.hardwarezone.com.sg/threads/omg-us-sees-early-signal-of-pfizer-bivalent-covid-19-shots-link-to-stroke.6861239/', 'https://forums.hardwarezone.com.sg/threads/singapore-approves-pfizer-bivalent-jab-for-those-aged-12-and-above.6827257/', 'https://forums.hardwarezone.com.sg/threads/who-took-moderna-bivalent-as-their-booster-lip-lai.6840467/', 'https://forums.hardwarezone.com.sg/threads/im-impressed-with-the-usage-of-bivalent-jab-and-removal-of-vds-for-unvax.6823045/', 'https://forums.hardwarezone.com.sg/threads/adverse-effects-rare-after-bivalent-covid-19-vaccine-jabs-hsa-report.6868384/']
# links = ['https://forums.hardwarezone.com.sg/threads/bivalent-covid-19-vaccination-for-18-49-age-group-to-be-rolled-out-from-nov-7.6831404/', 'https://forums.hardwarezone.com.sg/threads/moh-will-invite-people-aged-18-to-49-for-bivalent-vaccination-later-in-2022.6825082/', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/', 'https://forums.hardwarezone.com.sg/threads/modernas-bivalent-covid-19-booster-vaccine-in-spore-by-end-sept-pfizers-being-evaluated.6809768/', 'https://forums.hardwarezone.com.sg/threads/u-s-says-pfizers-bivalent-covid-shot-may-be-linked-to-stroke-in-older-adults.6861221/', 'https://forums.hardwarezone.com.sg/threads/omg-us-sees-early-signal-of-pfizer-bivalent-covid-19-shots-link-to-stroke.6861239/page-4', 'https://forums.hardwarezone.com.sg/threads/oyk-moderna-spikevax-bivalent-vaccine-same-safety-profile-as-original-dose.6820475/', 'https://forums.hardwarezone.com.sg/threads/omg-us-sees-early-signal-of-pfizer-bivalent-covid-19-shots-link-to-stroke.6861239/', 'https://forums.hardwarezone.com.sg/threads/singapore-approves-pfizer-bivalent-jab-for-those-aged-12-and-above.6827257/', 'https://forums.hardwarezone.com.sg/threads/who-took-moderna-bivalent-as-their-booster-lip-lai.6840467/', 'https://forums.hardwarezone.com.sg/threads/im-impressed-with-the-usage-of-bivalent-jab-and-removal-of-vds-for-unvax.6823045/', 'https://forums.hardwarezone.com.sg/threads/adverse-effects-rare-after-bivalent-covid-19-vaccine-jabs-hsa-report.6868384/', 'https://forums.hardwarezone.com.sg/threads/u-s-fda-cdc-see-early-signal-of-pfizer-bivalent-covid-shots-link-to-stroke-reuters.6861322/', 'https://forums.hardwarezone.com.sg/threads/bivalent-vaccine-didnt-improve-anything-i-feel.6836504/', 'https://forums.hardwarezone.com.sg/threads/glgt-moh-brings-forward-rollout-of-covid-19-bivalent-vaccine-amid-very-fast-rise-in-cases-of-new-omicron-variant.6821278/page-2', 'https://forums.hardwarezone.com.sg/threads/serious-i-have-heart-palpitation-after-28-dec-bivalent-hootz.6859670/', 'https://forums.hardwarezone.com.sg/threads/mrna-bivalent-vaccines-efficacy-against-latest-xbb-1-5-variant.6862815/', 'https://forums.hardwarezone.com.sg/threads/glgt-ong-ye-kung-urges-more-people-especially-seniors-to-get-bivalent-jab-amid-covid-19-mutation-risk-as-china-opens-up.6847326/', 'https://forums.hardwarezone.com.sg/threads/glgt-singapore-grants-interim-authorisation-for-first-bivalent-covid-19-booster-vaccine.6809184/page-4', 'https://forums.hardwarezone.com.sg/threads/386-000-bivalent-covid-19-vaccines-given-out-in-singapore-since-october-mobile-teams-to-boost-elderly-sign-ups.6847292/', 'https://forums.hardwarezone.com.sg/threads/over-586-000-people-have-taken-a-bivalent-covid-19-jab-moh.6856400/', 'https://forums.hardwarezone.com.sg/threads/glgt-bivalent-vaccine-gives-stronger-and-broader-protection-against-all-covid-19-strains.6823030/', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-6', 'https://forums.hardwarezone.com.sg/threads/gpgt-dpm-lawrence-wong-taking-moderna-bivalent-jab-this-morning.6833775/', 'https://forums.hardwarezone.com.sg/threads/glgt-ong-ye-kung-urges-more-people-especially-seniors-to-get-bivalent-jab-amid-covid-19-mutation-risk-as-china-opens-up.6847326/page-2', 'https://forums.hardwarezone.com.sg/threads/if-youre-young-and-fit-will-you-take-the-bivalent-jab.6861053/page-2', 'https://forums.hardwarezone.com.sg/threads/bivalent-moderna-covid-19-vaccine-available-in-singapore-oct-14-three-days-ahead-of-schedule.6821168/', 'https://forums.hardwarezone.com.sg/threads/glgt-moh-brings-forward-rollout-of-covid-19-bivalent-vaccine-amid-very-fast-rise-in-cases-of-new-omicron-variant.6821278/', 'https://forums.hardwarezone.com.sg/threads/moh-will-invite-people-aged-18-to-49-for-bivalent-vaccination-later-in-2022.6825082/page-6', 'https://forums.hardwarezone.com.sg/threads/glgt-singapore-grants-interim-authorisation-for-first-bivalent-covid-19-booster-vaccine.6809184/', 'https://forums.hardwarezone.com.sg/threads/glgt-singapore-grants-interim-authorisation-for-first-bivalent-covid-19-booster-vaccine.6809184/page-2', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-2', 'https://forums.hardwarezone.com.sg/threads/u-s-says-pfizers-bivalent-covid-shot-may-be-linked-to-stroke-in-older-adults.6861221/page-3', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-3', 'https://forums.hardwarezone.com.sg/threads/glgt-bivalent-vaccine-gives-stronger-and-broader-protection-against-all-covid-19-strains.6823030/page-2', 'https://forums.hardwarezone.com.sg/threads/glgt-vaccination-centres-busy-on-first-day-of-bivalent-vaccine-roll-out.6822939/page-2', 'https://forums.hardwarezone.com.sg/threads/ama-took-my-pfizer-bivalant-the-bestest-2nd-booster-in-sg.6849981/', 'https://forums.hardwarezone.com.sg/threads/what-is-the-bivalent-vaccine-jab-is-it-better-or-worse-than-pfizer.6837976/', 'https://forums.hardwarezone.com.sg/threads/what-the-media-didnt-tell-us-abt-bivalent-covid19-vax-ah.6809577/', 'https://forums.hardwarezone.com.sg/threads/oyk-moderna-spikevax-bivalent-vaccine-same-safety-profile-as-original-dose.6820475/page-2', 'https://forums.hardwarezone.com.sg/threads/bivalent-covid-19-vaccination-for-18-49-age-group-to-be-rolled-out-from-nov-7.6831404/page-2', 'https://forums.hardwarezone.com.sg/threads/bivalent-vaccine-booster.6822828/', 'https://forums.hardwarezone.com.sg/threads/how-long-after-getting-covid-can-we-go-take-the-bivalent-booster-shot.6856581/', 'https://forums.hardwarezone.com.sg/threads/glgt-effectiveness-of-covid-19-bivalent-vaccine-cleveland-clinic-study.6858061/', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-7', 'https://forums.hardwarezone.com.sg/threads/glgt-singapore-grants-interim-authorisation-for-first-bivalent-covid-19-booster-vaccine.6809184/page-3', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-11', 'https://forums.hardwarezone.com.sg/threads/mrna-bivalent-vaccines-efficacy-against-latest-xbb-1-5-variant.6862815/page-2', 'https://forums.hardwarezone.com.sg/threads/moh-will-invite-people-aged-18-to-49-for-bivalent-vaccination-later-in-2022.6825082/page-5', 'https://forums.hardwarezone.com.sg/threads/a-new-study-from-germany-points-to-more-adverse-reactions-following-bivalent-covid-19-mrna-booster-vaccine.6837967/', 'https://forums.hardwarezone.com.sg/threads/if-youre-young-and-fit-will-you-take-the-bivalent-jab.6861053/', 'https://forums.hardwarezone.com.sg/threads/need-to-wait-how-long-from-past-infection-to-take-bivalent-covid-vaccine.6832512/', 'https://forums.hardwarezone.com.sg/threads/glgt-ong-ye-kung-urges-more-people-especially-seniors-to-get-bivalent-jab-amid-covid-19-mutation-risk-as-china-opens-up.6847326/page-5', 'https://forums.hardwarezone.com.sg/threads/modernas-bivalent-covid-19-booster-vaccine-in-spore-by-end-sept-pfizers-being-evaluated.6809768/page-2', 'https://forums.hardwarezone.com.sg/threads/glgt-moh-brings-forward-rollout-of-covid-19-bivalent-vaccine-amid-very-fast-rise-in-cases-of-new-omicron-variant.6821278/page-4', 'https://forums.hardwarezone.com.sg/threads/is-it-ok-to-do-exercise-with-gf-wife-the-night-after-taking-bivalent-vaccine.6840666/', 'https://forums.hardwarezone.com.sg/threads/have-you-taken-your-bivalent-booster.6836443/', 'https://forums.hardwarezone.com.sg/threads/omg-us-sees-early-signal-of-pfizer-bivalent-covid-19-shots-link-to-stroke.6861239/page-2', 'https://forums.hardwarezone.com.sg/threads/bivalent-vaccine-didnt-improve-anything-i-feel.6836504/page-2', 'https://forums.hardwarezone.com.sg/threads/glgt-vaccination-centres-busy-on-first-day-of-bivalent-vaccine-roll-out.6822939/', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-4', 'https://forums.hardwarezone.com.sg/threads/serious-i-have-heart-palpitation-after-28-dec-bivalent-hootz.6859670/page-7', 'https://forums.hardwarezone.com.sg/threads/bivalent-covid-19-vaccine-will-be-offered-to-healthcare-workers-in-singapore-from-oct-25.6825660/', 'https://forums.hardwarezone.com.sg/threads/serious-i-have-heart-palpitation-after-28-dec-bivalent-hootz.6859670/page-8', 'https://forums.hardwarezone.com.sg/threads/serious-i-have-heart-palpitation-after-28-dec-bivalent-hootz.6859670/page-3', 'https://forums.hardwarezone.com.sg/threads/dailymail-fda-and-cdc-advisors-accuse-moderna-of-withholding-trial-data-that-suggested-its-covid-bivalent-booster-was-less-effective-than-older-shot.6860916/', 'https://forums.hardwarezone.com.sg/threads/glgt-ong-ye-kung-urges-more-people-especially-seniors-to-get-bivalent-jab-amid-covid-19-mutation-risk-as-china-opens-up.6847326/page-4', 'https://forums.hardwarezone.com.sg/threads/serious-i-have-heart-palpitation-after-28-dec-bivalent-hootz.6859670/page-2', 'https://forums.hardwarezone.com.sg/threads/serious-i-have-heart-palpitation-after-28-dec-bivalent-hootz.6859670/page-6', 'https://forums.hardwarezone.com.sg/threads/singapore-data-for-bivalent-covid-19-vaccines-does-not-show-increased-risk-of-stroke-moh.6861552/page-10', 'https://forums.hardwarezone.com.sg/threads/bivalent-covid-19-vaccination-for-18-49-age-group-to-be-rolled-out-from-nov-7.6831404/page-4', 'https://forums.hardwarezone.com.sg/threads/moh-will-invite-people-aged-18-to-49-for-bivalent-vaccination-later-in-2022.6825082/page-3', 'https://forums.hardwarezone.com.sg/threads/kenneth-mak-steps-are-being-taken-to-bring-modernas-new-bivalent-vaccine-once-available-cover-both-wildtype-covid-19-omicron-variant.6798931/', 'https://forums.hardwarezone.com.sg/threads/bivalent-moderna-covid-19-vaccine-available-in-singapore-oct-14-three-days-ahead-of-schedule.6821168/page-2', 'https://forums.hardwarezone.com.sg/threads/roll-out-of-moderna-spikevax-bivalent-covid-19-vaccine-brought-forward-to-oct-14-the-big-story.6821289/', 'https://forums.hardwarezone.com.sg/threads/17-flights-from-china-arriving-singapore-these-2-days-i-better-go-take-bivalent.6859550/', 'https://forums.hardwarezone.com.sg/threads/glgt-longer-waiting-times-up-to-2-hours-for-bivalent-jab-as-more-people-head-to-vaccination-centres.6824340/', 'https://forums.hardwarezone.com.sg/threads/glgt-answer-is-yes-five-months-after-their-second-booster-they-are-recommended-to-take-the-bivalent-vaccine-he-said.6825134/', 'https://forums.hardwarezone.com.sg/threads/oyk-i-have-received-several-questions-since-fridays-mtf-announcement.6820426/', 'https://forums.hardwarezone.com.sg/threads/will-u-take-the-4th-vaccine-shot.6835866/page-2', 'https://forums.hardwarezone.com.sg/threads/what-are-bivalent-vaccines-do-i-need-a-second-booster-heres-what-you-need-to-know-about-singapores-new-covid-19-vaccination-strategy.6821184/', 'https://forums.hardwarezone.com.sg/threads/u-s-says-pfizers-bivalent-covid-shot-may-be-linked-to-stroke-in-older-adults.6861221/page-2', 'https://forums.hardwarezone.com.sg/threads/2nd-booster-shot-side-effect-is-it-very-gao-lat.6849721/page-2', 'https://forums.hardwarezone.com.sg/threads/glgt-you-should-skip-the-second-covid-19-booster-shot-and-take-the-bivalent-vaccine-instead-says-ong-ye-kung.6820343/', 'https://forums.hardwarezone.com.sg/threads/st-singapore-grants-approval-for-pfizers-bivalent-vaccine-vaccines-for-under-5s-begin.6827366/', 'https://forums.hardwarezone.com.sg/threads/mr-yong-and-his-wife-took-their-fifth-shots-at-the-kaki-bukit-jtvc-where-the-waiting-time-stretched-up-to-two-hours.6824256/', 'https://forums.hardwarezone.com.sg/threads/sms-lai-liao.6832558/page-3', 'https://forums.hardwarezone.com.sg/threads/glgt-those-who-have-received-their-second-booster-should-be-offered-a-booster-dose-of-the-bivalent-vaccine-around-5-months-after-their-last-booster.6819786/', 'https://forums.hardwarezone.com.sg/threads/ong-added-that-the-review-on-vds-should-be-completed-in-the-next-couple-of-months.6808822/', 'https://forums.hardwarezone.com.sg/threads/moh-will-therefore-replace-the-original-moderna-spikevax-vaccines-with-the-updated-bivalent-version-from-17-oct-2022-sinovac-can-be-used-for-booster.6819711/page-2', 'https://forums.hardwarezone.com.sg/threads/going-japan-this-oct-dec.6221719/page-63', 'https://forums.hardwarezone.com.sg/threads/17-flights-from-china-arriving-singapore-these-2-days-i-better-go-take-bivalent.6859550/page-3', 'https://forums.hardwarezone.com.sg/threads/bloomberg-new-covid-boosters-arent-better-than-old-ones-study-finds.6829647/', 'https://forums.hardwarezone.com.sg/threads/poll-covid-4th-booster-jab-is-it-really-necessary.6844156/', 'https://forums.hardwarezone.com.sg/threads/people-think-they-dont-need-a-fourth-covid-19-shot-theyre-wrong.6842906/', 'https://forums.hardwarezone.com.sg/threads/2022-second-booster-shoot.6836652/', 'https://forums.hardwarezone.com.sg/threads/rules-tied-to-covid-19-vaccination-status-to-be-lifted-from-oct-10-as-spore-switches-to-up-to-date-vaccination-policy.6819756/', 'https://forums.hardwarezone.com.sg/threads/jab-5-times-still-tio-covid.6853477/page-2', 'https://forums.hardwarezone.com.sg/threads/why-oyk-so-satki-ah-among-the-fastest-in-the-world-to-review-the-moderna-bivalent-vaccine.6809826/', 'https://forums.hardwarezone.com.sg/threads/17-flights-from-china-arriving-singapore-these-2-days-i-better-go-take-bivalent.6859550/page-6']

#Run on local only cause might face captcha verification
# options = webdriver.ChromeOptions()
# options.add_experimental_option('excludeSwitches', ['enable-logging'])

# driver = webdriver.Chrome(service=Service(
#     executable_path=ChromeDriverManager().install()), options=options)
# driver.get('https://www.hardwarezone.com.sg/search/forum/Bivalent')

# while count <= 10:
#     time.sleep(2)
#     resultBoxElement = driver.find_element(
#         By.CLASS_NAME, 'gsc-resultsbox-visible')
#     URLtitle = resultBoxElement.find_elements(By.CLASS_NAME, 'gs-title')
#     for x in URLtitle:
#         link = x.get_attribute('data-ctorig')
#         if link != None:
#             links.append(link)
#     count = count + 1
#     if count <= 10:
#         driver.find_element(
#             By.XPATH, '//*[@id="___gcse_0"]/div/div/div/div[5]/div[2]/div/div/div[2]/div/div['+str(count)+']').click()
        
# links = list(dict.fromkeys(links))
# links = list(set(links))
# print(links)

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
            date_obj = dt.datetime.strptime(extracted_date.text, '%b %d, %Y')
            formatted_date = date_obj.strftime('%b %Y')
            data_dictionary["date"].append(formatted_date)
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

csv_buffer = hwz_df.to_json(orient='records').encode()
csv_file = BytesIO(csv_buffer)

s3 = boto3.resource('s3')
bucket_name = 'is459-g1t8-project'  # replace this with your S3 bucket name
object_key = 'input/hwz.json'  # the key under which the object will be stored in the S3 bucket
s3.Bucket(bucket_name).upload_fileobj(csv_file, object_key)



# # schedule the job to run once every day at a specific time
# schedule.every().day.at('12:00').do(download_and_upload_comments)

# # run the scheduled job
# while True:
#     schedule.run_pending()
#     time.sleep(1)