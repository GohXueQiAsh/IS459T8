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
# https://www.facebook.com/sghealthministry/posts/take-your-booster-as-soon-as-you-are-eligible-here-are-some-answers-to-guide-you/317010660454364/
POST_ID = "317010660454364"

# number of comments to download -- set this to True to download all comments
MAX_COMMENTS = 200

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

myFile = open('firstcovidnews.csv', 'w', encoding='utf-8')
writer = csv.writer(myFile)
writer.writerow(['comment_id','comment_url',	'commenter_id',	'commenter_url','commenter_name' ,'commenter_meta','comment_text','comment_time',	'comment_image','comment_reactors',	'comment_reactions','comment_reaction_count','replies'])
for dictionary in comments:
    writer.writerow(dictionary.values())
myFile.close()
#myFile = open('firstcovidnews.csv', 'r')
#myFile.close()

with open('firstcovidnews.csv', 'rb') as file:
    csv_file = BytesIO(file.read())

s3 = boto3.resource('s3')
bucket_name = 'is459-g1t8-project'  # replace this with your S3 bucket name
object_key = 'input/firstcovidnews.csv'  # the key under which the object will be stored in the S3 bucket
s3.Bucket(bucket_name).upload_fileobj(csv_file, object_key)

#     # e.g. ...get the replies for them
#     for reply in comment['replies']:
#         print(' ', reply)