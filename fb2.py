"""
Download comments for a public Facebook post.
"""
import facebook_scraper as fs
import csv
import boto3
from io import BytesIO

# get POST_ID from the URL of the post which can have the following structure:
# https://www.facebook.com/USER/posts/POST_ID
# https://www.facebook.com/groups/GROUP_ID/posts/POST_ID
# https://www.facebook.com/sghealthministry/posts/are-you-armed-with-a-booster-shot-yet-from-14-feb-2022-a-booster-shot-is-necessa/320010360154394/
POST_ID = "320010360154394"

# number of comments to download -- set this to True to download all comments
MAX_COMMENTS = 100

# get the post (this gives a generator)
gen = fs.get_posts(
    post_urls=[POST_ID],
    options={"comments": MAX_COMMENTS, "progress": True},
    credentials=('is459g1t8@gmail.com' ,"qwer1234!")
)

# take 1st element of the generator which is the post we requested
post = next(gen)

# extract the comments part
comments = post['comments_full']

# process comments as you want...

myFile = open('2covidnews.csv', 'w')
writer = csv.writer(myFile)
writer.writerow(['comment_id','comment_url',	'commenter_id',	'commenter_url','commenter_name' ,'commenter_meta','comment_text','comment_time',	'comment_image','comment_reactors',	'comment_reactions','comment_reaction_count','replies'])
for dictionary in comments:
    writer.writerow(dictionary.values())
myFile.close()
#myFile = open('2covidnews.csv', 'r')
#myFile.close()

with open('2covidnews.csv', 'rb') as file:
    csv_file = BytesIO(file.read())

s3 = boto3.resource('s3')
bucket_name = 'is459-g1t8-project'  # replace this with your S3 bucket name
object_key = 'input/2covidnews.csv'  # the key under which the object will be stored in the S3 bucket
s3.Bucket(bucket_name).upload_fileobj(csv_file, object_key)
    

#     # e.g. ...get the replies for them
#     for reply in comment['replies']:
#         print(' ', reply)