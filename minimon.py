from datetime import date, timedelta
from boto.s3.key import Key
import tweepy, boto.s3, boto, urllib.request, os, json

def extrH(l):
    return l['text']

def extrI(l):
    return l['media_url']

def pushImages(url,who,bucket):
    localFile = who+'-'+url.split("/")[-1]
    urllib.request.urlretrieve(url, '/tmp/'+localFile)
    k = Key(bucket)
    k.key = 'pictures/'+localFile
    k.set_contents_from_filename('/tmp/'+localFile)
    return True

def pushTweet(localFile,bucket):
    k = Key(bucket)
    k.key = 'json/'+localFile
    k.set_contents_from_filename('/tmp/'+localFile)
    return True

def writeTweet(tweet, bucket):
    id = str(tweet.id)
    who = tweet.user.screen_name
    when = str(tweet.created_at - timedelta(0,18000))
    what = tweet.full_text
    hashtags = [extrH(hash) for hash in tweet.entities['hashtags']]
    images = [extrI(image) for image in tweet.extended_entities['media']]
    retweets = tweet.retweet_count
    favorites = tweet.favorite_count
    following = tweet.user.friends_count
    followers = tweet.user.followers_count
    [pushImages(url,id,bucket) for url in images]
    body = {'id':id,'who':who, 'when':when, 'text':what, 'hashtags':hashtags, 'images':images, 'retweets':retweets, 'favorites':favorites, 'following':following, 'followers':followers}
    localFile = id+'.json'
    with open('/tmp/'+localFile,'w') as outfile:
        json.dump(body,outfile)
    pushTweet(localFile,bucket)
    return True

def lambda_handler(event, context):
    firstDate = (date.today()-timedelta(7)).strftime("%Y-%m-%d")
    lastDate = (date.today()-timedelta(6)).strftime("%Y-%m-%d")

    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''

    s3conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = s3conn.get_bucket('miniature-monday')

    consumer_key=''
    consumer_secret=''
    access_token_key=''
    access_token_secret=''

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)

    api = tweepy.API(auth)

    cursor = tweepy.Cursor(api.search, q='#miniaturemonday filter:safe', include_entities=True, since=firstDate, until=lastDate, tweet_mode='extended')

    image_tweets = 0
    total_tweets = 0

    for tweet in list(cursor.items()):
        total_tweets += 1
        if (not tweet.retweeted) and ('RT' not in tweet.full_text):
            if 'media' in tweet.entities:
                writeTweet(tweet,bucket)
                print('On '+tweet.created_at.strftime('%m/%d/%Y')+' '+tweet.user.screen_name+' tweeted some images!')
                image_tweets += 1

    print('Done!')
    print(str(total_tweets)+' Total Tweets')
