"""
Download comments for a public Facebook post.
"""
import facebook_scraper as fs
import csv
import boto3
from io import BytesIO
import time
import schedule
import datetime
import pandas as pd
# get POST_ID from the URL of the post which can have the following structure:
# https://www.facebook.com/USER/posts/POST_ID
# https://www.facebook.com/groups/GROUP_ID/posts/POST_ID
# https://www.facebook.com/ChannelNewsAsia/posts/singapore-is-reviewing-the-safety-and-effectiveness-of-covid-19-booster-vaccines/10159008998987934/

# def download_and_upload_comments():
#     today = datetime.datetime.today()
#     if today.day == 31:
POST_ID = "10159008998987934"

# number of comments to download -- set this to True to download all comments
MAX_COMMENTS = 100

# get the post (this gives a generator)
gen = fs.get_posts(
    post_urls=[POST_ID],
    options={"comments": MAX_COMMENTS, "progress": True},
    credentials=('is459g1t8@gmail.com' ,"qwer1234@")
)

# take 1st element of the generator which is the post we requested
post = next(gen)

# extract the comments part
comments = post['comments_full']

# process comments as you want..
df = pd.DataFrame.from_dict(comments)
df.drop(labels=['comment_id', 'comment_url', 'commenter_id','commenter_url','commenter_meta','comment_image',\
                      'comment_reactors','comment_reactions', 'replies'], axis='columns', inplace=True)
df.rename(columns={'commenter_name': 'author', 'comment_text': 'body', 'comment_reaction_count': 'score', 'comment_time': 'date'}, inplace=True)
df['source'] = 'fb'
df['score'].fillna('0', inplace=True)
df['date'] = pd.to_datetime(df['date'])
df['date'] = df['date'].dt.strftime('%B %Y')
df.to_json("fb4.json", orient='records')

with open('fb4.json', 'rb') as file:
    json_file = BytesIO(file.read())

s3 = boto3.resource('s3')
bucket_name = 'raw--data-is459'  # replace this with your S3 bucket name
object_key = 'read/fb4.json'  # the key under which the object will be stored in the S3 bucket
s3.Bucket(bucket_name).upload_fileobj(json_file, object_key)

# schedule.every().day.at('12:00').do(download_and_upload_comments)

# # run the scheduled job
# while True:
#     schedule.run_pending()
#     time.sleep(1)
#     # e.g. ...get the replies for them
#     for reply in comment['replies']:
#         print(' ', reply)
