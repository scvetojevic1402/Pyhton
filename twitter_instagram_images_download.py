import geocoder
import time
import sys
import ujson as json
import oauth2
from BeautifulSoup import BeautifulSoup
import os
import urllib2
import re

CONSUMER_KEY = "yourConsumerKey";
CONSUMER_SECRET = "yourConsumerSecret";
ACCESS_TOKEN = "yourAccessToken";
ACCESS_SECRET = "yourAccessSecret";

def oauth_req(url, key, secret, http_method='GET', post_body='', http_headers=None):
    consumer = oauth2.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth2.Token(key=key, secret=secret)
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, method=http_method, body=post_body, headers=http_headers )
    return content
def get_users_tweets(username):
    tweets = json.loads(oauth_req('https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name={}&exclude_replies=false&count=3200'.format(username),ACCESS_TOKEN, ACCESS_SECRET))
    return tweets
#tweets = get_recents_tweets("Boston","elections2016",100)
def insta_photos_download(pics_path,user):
    tweets = get_users_tweets(user)
    insta_photos_urls=[]
    for tweet in tweets:
        if tweet.has_key('entities')==True and (tweet['entities']).has_key('urls')==True:
            for photo in tweet['entities']['urls']:
                insta_photos_urls.append(photo['expanded_url'])
    i=0
    for url in insta_photos_urls:
        try:
            site = urllib2.urlopen(url)
            twitter_pic = site.read()
            soup = BeautifulSoup(twitter_pic)
            for elem in soup.findAll(['script',{'location'}],text=re.compile('window._sharedData')):
                elem.extract()
                shared_data = elem.string.partition('=')[-1].strip(' ;')
                location = json.loads(shared_data)
                if (location['entry_data']['PostPage'][0]['media']).has_key('location') and location['entry_data']['PostPage'][0]['media']['location'] is not None:
                    if (location['entry_data']['PostPage'][0]['media']['location']).has_key('name') and location['entry_data']['PostPage'][0]['media']['location']['name'] is not None:
                        location_name=location['entry_data']['PostPage'][0]['media']['location']['name']
                        folder_name = '{0}\\{1}\\'.format(user,location_name)
                        if not os.path.exists(pics_path+folder_name):
                            os.makedirs(pics_path+folder_name, 777)
                        og_image=soup.findAll('meta', {"property":'og:image'})
                        try:
                            image = {og_image[0]['content']}
                            instapic = urllib2.urlopen(image.pop()).read()
                            output = open(pics_path+folder_name+'{0}.jpg'.format(i),'wb')
                            output.write(instapic)
                            output.close()
                            i+=1
                        except Exception as e:
                            print('No img found')
                            pass
        except Exception as e:
            print('No img found')
            pass

pics_path='C:\\Users\\User\\Documents\\Tweets_analysis\\Pictures\\insta_photos\\'
user="sweatengine" #an example of a public user with many travel photos
insta_photos_download(pics_path,user)
