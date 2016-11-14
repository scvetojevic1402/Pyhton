#THIS SCRIPT 
import psycopg2 as pc
import urllib
import urllib2
from BeautifulSoup import BeautifulSoup
import time
import sys
import pandas as pd
import thread


parsing_start=time.time()
con1 = pc.connect("host=hostname user=User password=Pass port=5433 dbname=db")
con1.autocommit=True
c1 = con1.cursor()
c1.execute("SET CLIENT_ENCODING TO 'LATIN1';")
c2 = con1.cursor()
c2.execute("SET CLIENT_ENCODING TO 'LATIN1';")
c1.execute("SELECT 'https://www.twitter.com/statuses/'||tweet_id,tweet_id,ntile(25000) over(order by tweet_id asc) as ntile\
            FROM hashtags_paris_world WHERE RETWEETS IS NULL ORDER BY 1;") #and tweet_id = 665277903236636672


urls = c1.fetchall()
arr=pd.DataFrame(urls)
arr.columns=['url','tweet_id','ntile']
print("Row count: {0}".format(c1.rowcount))

def records_processing(idx):
    x=0
    y=0
    print(type(arr[arr['ntile']==idx]))  
    for row in arr[arr['ntile']==idx].iterrows():
        print(row[1]['url'])
        #site = urllib.urlopen('https://twitter.com/statuses/{0}'.format(row[0]))
        site = urllib.urlopen(row[1]['url'])
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
        query="UPDATE hashtags_paris_world SET RETWEETS = {0}, FAVORITES = {1} WHERE tweet_id = {2};".format(retweet_count,favourites_count,row[1]['tweet_id'])
        x=x+1
        c2.execute(query)
        if(x%10==0): 
            print("Thread: {0}, Processed: {1}, Found: {2} ...".format(idx, x,y))
        #except Exception as e:
        #    print("Line: {0} Error: {1}".format(sys.exc_info()[-1].tb_lineno,e))
        #    continue
            con1.commit()
        return 
#print(arr[arr['ntile']==4])
for i in range(1,5):
    print(i)
    thread.start_new_thread(records_processing,(i,))


#print("Processed: {0}, Found: {1} ...".format(x,y))
con1.commit()
c1.close()
c2.close()
