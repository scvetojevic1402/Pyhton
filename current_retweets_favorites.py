#streaming api returns tweets soon after they were posted so numbers of retweets and favorites are both 0
#this script parses HTML of tweet's webpage to get current numbers of retweets and favorites and updates the table containing tweets
import psycopg2 as pc
import urllib
import urllib2
from BeautifulSoup import BeautifulSoup
import time
import sys

x=0
y=0
parsing_start=time.time()
con1 = pc.connect("host=Host user=User password=Pass dbname=Db")
con1.autocommit=True
c1 = con1.cursor()
c2 = con1.cursor()
c1.execute("SELECT 'https://www.twitter.com/statuses/'||tweet_id,tweet_id FROM SomeTableWithTweets WHERE RETWEETS IS NULL ORDER BY 1;")


urls = c1.fetchall()
print("Row count: {0}".format(c1.rowcount))
for row in urls:
    try:
        site = urllib.urlopen(row[0])
        twitter_pic = site.read()
        soup = BeautifulSoup(twitter_pic)
        retweets_div=soup.findAll('a', {"class":'request-retweeted-popup'})
        favourites_div=soup.findAll('a', {"class":'request-favorited-popup'})
        if(len(retweets_div)>0):
            retweet_count = retweets_div[0].get('data-tweet-stat-count')
        else:
            retweet_count = 0
        if(len(favourites_div)>0):
            favourites_count = favourites_div[0].get('data-tweet-stat-count')
        else:
            favourites_count=0
        if(retweet_count > 0 or favourites_count > 0):
            y=y+1
        query="UPDATE SomeTableWithTweets SET RETWEETS = {0}, FAVOURITES = {1} WHERE tweet_id = {2};".format(retweet_count,favourites_count,row[1])
        x=x+1
        c2.execute(query)
        if(x%100==0): 
            print("Processed: {0}, Found: {1} ...".format(x,y))
    except Exception as e:
        print("Line: {0} Error: {1}".format(sys.exc_info()[-1].tb_lineno,e))
        continue
	con1.commit()
print("Processed: {0}, Found: {1} ...".format(x,y))
con1.commit()
c1.close()
c2.close()
con1.close()
