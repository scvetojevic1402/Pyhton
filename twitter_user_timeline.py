#CHECK IF USERS THAT I DON'T HAVE IN THE DATABASE OF GEO-TAGGED TWEETS HAS ANY USING THE SEARCH API
import oauth2
import json
import psycopg2 as pc
import psycopg2.extras
import time
import sys

conn = pc.connect("host=Host user=User password=Pass dbname=Db")
c1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
c2 = conn.cursor()

c1.execute("with cte as (select tweet_id,tweet#>'{user,id}' as user_id\
            ,row_number() over(partition by tweet#>'{user,id}' order by tweet_id asc) as rn\
           from paris_all where useful = 't' order by 1 asc) select tweet_id, user_id from cte where rn=1\
           and user_id not in (select distinct tweet#>'{user,id}' from user_timeline);")


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

y=0
for row in c1:
    y=y+1
    print(row["user_id"])
    response = oauth_req('https://api.twitter.com/1.1/statuses/user_timeline.json?user_id={0}&since_id={0}&count=5000'.format(row["user_id"],row["tweet_id"]-1),ACCESS_TOKEN, ACCESS_SECRET)
    timeline = json.loads(response)
    i = 0
    for tweet in timeline:
        sql = "INSERT INTO USER_TIMELINE(TWEET) VALUES (replace(convert_from(convert_to('{0}','LATIN1'),'UTF8'),'\u0000','')::jsonb)".format(str(json.dumps(tweet)).replace("'","''"))
        c2.execute(sql)
        i=i+1
    conn.commit()
    print("Users: {0}, tweets: {1}".format(y,i))
conn.close()
