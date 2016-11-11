#THIS SCRIPT FETCHES USER_IDs FROM DATABASE AND DOWNLOADS ALL THE FOLLOWERS OF EACH USER_ID
#IN CASE OF THE API THRESHOLD IS REACHED THE IT WILL WAIT AND CALL THE API AGAIN IN 5 MINUTES
#QUERY TO FETCH THE USER_IDs CAN BE CHANGED AS NEEDED
import oauth2
import json
import psycopg2 as pc
import psycopg2.extras
import time
import sys


conn = pc.connect("host=YourHost user=YourUser password=YourPass dbname=YourDB")
c1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
c2 = conn.cursor()
c1.execute("WITH CTE AS (SELECT DISTINCT TWEET#>'{user,screen_name}' AS user_name,TWEET#>'{user,id}' AS user_id,(TWEET#>>'{user,followers_count}')::int as followers_count\
         ,row_number() over(partition by  TWEET#>'{user,screen_name}',TWEET#>'{user,id}' order by (TWEET#>>'{user,followers_count}')::int asc)\
             FROM PARIS_ALL A WHERE EXISTS (SELECT TWEET_ID FROM HASHTAGS_PARIS_ALL WHERE TWEET_ID = A.TWEET_ID AND hashtag = 'ParisAttacks'\
              ) AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT)\
             AND NOT EXISTS (SELECT USER_ID FROM FOLLOWERS_OF_FOLLOWERS WHERE USER_ID = (A.TWEET#>>'{user,id}')::BIGINT) AND RETWEETS > 0\
             ORDER BY (TWEET#>>'{user,followers_count}')::int ASC)\
             SELECT * FROM CTE WHERE ROW_NUMBER=1;")
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

for row in c1:
    cursor = -1
    print(row["user_name"])
    response = oauth_req('https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000'.format(cursor,row["user_name"]),ACCESS_TOKEN, ACCESS_SECRET)
    followers = json.loads(response)
    print(followers)
    i = 0
    while(followers.has_key('errors') == True and str(followers['errors'][0]['message'])=='Rate limit exceeded'):
        print('Sleeping for 5 min...')
        time.sleep(300)
        followers = json.loads(oauth_req('https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000'.format(cursor,row["user_name"]),ACCESS_TOKEN, ACCESS_SECRET))
        print(followers)
    if(followers.has_key('errors') == True):
        print(str(followers['errors'][0]['message']))
    if(followers.has_key('errors') == True and str(followers['errors'][0]['message'])=='Sorry, that page does not exist.'):
        sql = "INSERT INTO FOLLOWERS(USER_ID,FOLLOWER_ID) VALUES ({0},{1})".format(row["user_id"],0)
        c2.execute(sql)
    if (followers.has_key("ids")):
        print(len(followers['ids']))
        next_cursor = followers['next_cursor']
        for id in followers['ids']:
            sql = "INSERT INTO FOLLOWERS(USER_ID,FOLLOWER_ID) VALUES ({0},{1})".format(row["user_id"],id)
            c2.execute(sql)
            i=i+1
            if(i%1000==0):
                print(i)
        while(next_cursor!=0):
            response = oauth_req('https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000'.format(next_cursor,row["user_name"]),ACCESS_TOKEN, ACCESS_SECRET)
            followers = json.loads(response)
            print(followers)
            while(followers.has_key('errors') == True and str(followers['errors'][0]['message'])=='Rate limit exceeded'):
                print('Sleeping for 5 min...')
                time.sleep(300)
                followers = json.loads(oauth_req('https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000'.format(cursor,row["user_name"]),ACCESS_TOKEN, ACCESS_SECRET))
                print(followers)
            next_cursor = followers['next_cursor']
            for id in followers['ids']:
                sql = "INSERT INTO FOLLOWERS(USER_ID,FOLLOWER_ID) VALUES ({0},{1})".format(row["user_id"],id)
                c2.execute(sql)
                i=i+1
                if(i%1000==0):
                    print(i)
        #print(i)
        conn.commit()
    else:
        sql = "INSERT INTO FOLLOWERS(USER_ID,FOLLOWER_ID) VALUES ({0},{1})".format(row["user_id"],0)
        c2.execute(sql)
        conn.commit()
        print("Profile private or deleted")
