"""
Download comments for a public Facebook post.
"""
import facebook_scraper as fs
import csv

# get POST_ID from the URL of the post which can have the following structure:
# https://www.facebook.com/USER/posts/POST_ID
# https://www.facebook.com/groups/GROUP_ID/posts/POST_ID
# https://www.facebook.com/cdc/posts/292035502957895/?paipv=0&eav=AfbxuhrKSY2vFQR0Gxb7CUg_F2ftLgAHq9dXc9gW-kTwXHrd461rluDH67F_xPR0EZ0&_rdr
POST_ID = "292035502957895"

# number of comments to download -- set this to True to download all comments
MAX_COMMENTS = 100

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

myFile = open('3covidnews.csv', 'w')
writer = csv.writer(myFile)
writer.writerow(['comment_id','comment_url',	'commenter_id',	'commenter_url','commenter_name' ,'commenter_meta','comment_text','comment_time',	'comment_image','comment_reactors',	'comment_reactions','comment_reaction_count','replies'])
for dictionary in comments:
    writer.writerow(dictionary.values())
myFile.close()
myFile = open('3covidnews.csv', 'r')
myFile.close()
    

#     # e.g. ...get the replies for them
#     for reply in comment['replies']:
#         print(' ', reply)
# https://www.facebook.com/ChannelNewsAsia/posts/singapore-is-reviewing-the-safety-and-effectiveness-of-covid-19-booster-vaccines/10159008998987934/