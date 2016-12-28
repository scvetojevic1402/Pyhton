#streamin api does not return retweets by default... this script can download up to 20 retweets (API limit)
import oauth2
import json
import psycopg2 as pc
import time
import sys

pc.extensions.register_type(pc.extensions.UNICODE)
pc.extensions.register_type(pc.extensions.UNICODEARRAY)

conn = pc.connect("host=Host user=User password=Pass dbname=Db")
c1 = conn.cursor()
c2 = conn.cursor()
c1.execute("SELECT TWEET_ID FROM SomeTableWithTweets WHERE RETWEETS>0 AND TWEET_ID NOT IN (SELECT DISTINCT TWEET_ID FROM TableWithRetweets);")

def oauth_req(url, key, secret, http_method='GET', post_body='', http_headers=None):
    consumer = oauth2.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth2.Token(key=key, secret=secret)
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, method=http_method, body=post_body, headers=http_headers )
    return content

CONSUMER_KEY = "YourConsumerKey";
CONSUMER_SECRET = "YourConsumerSecret";
ACCESS_TOKEN = "YourAccessToken";
ACCESS_SECRET = "YourAccessSecret";

api_calls = 0
for row in c1:
    try:
        print(row)
        api_calls = api_calls + 1
        response = oauth_req('https://api.twitter.com/1.1/statuses/retweets/{0}.json'.format(row[0]),ACCESS_TOKEN, ACCESS_SECRET)
        tweets = json.loads(response)
        i = 0
        print(len(tweets))
        if(type(tweets) is dict):
            while(tweets.has_key('errors') == True and str(tweets['errors'][0]['message'])=='Rate limit exceeded'):
                print('Sleeping for 5 min...')
                time.sleep(300)
                tweets = json.loads(oauth_req('https://api.twitter.com/1.1/statuses/retweets/{0}.json'.format(row[0]),ACCESS_TOKEN, ACCESS_SECRET))
                print(tweets)
        
        if (len(tweets)> 0):
            while i < len(tweets):
                sql = "INSERT INTO TableWithRetweets(TWEET_ID,RETWEET) VALUES ({0},replace(convert_from(convert_to('{1}','LATIN1'),'UTF8'),'\u0000','')::jsonb)".format(row[0],str(json.dumps(tweets[i])).replace("'","''"))
                c2.execute(sql)
                conn.commit()
                i=i+1
    except Exception as e:
        print("Line: {0} Error: {1}".format(sys.exc_info()[-1].tb_lineno,e))
        continue
