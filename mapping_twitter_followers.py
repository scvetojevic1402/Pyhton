import geocoder
import time
import sys
import json
import oauth2
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np
from numpy import array
import matplotlib.colors as colors

CONSUMER_KEY = "YourConsumerKey";
CONSUMER_SECRET = "YourConsumerSecret";
ACCESS_TOKEN = "YourAccessToken";
ACCESS_SECRET = "YourAccessSecret";

def oauth_req(url, key, secret, http_method='GET', post_body='', http_headers=None):
    consumer = oauth2.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth2.Token(key=key, secret=secret)
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, method=http_method, body=post_body, headers=http_headers )
    return content
def call_followers_api(cursor,username):
    response = oauth_req('https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000'.format(cursor,username),ACCESS_TOKEN, ACCESS_SECRET)
    return response
def call_users_show_api_user_id(user_id):
    response = oauth_req('https://api.twitter.com/1.1/users/show.json?user_id={}'.format(user_id),ACCESS_TOKEN, ACCESS_SECRET)
    return response
def call_users_show_api_username(screen_name):
    response = oauth_req('https://api.twitter.com/1.1/users/show.json?screen_name={}'.format(screen_name),ACCESS_TOKEN, ACCESS_SECRET)
    return response
def geocode(place):
    return geocoder.arcgis('{0}'.format(place.encode('ascii','ignore')))
    #in next commit geocoder timeout will be handled too
def get_all_followers_ids(twitter_username):
    followers_list=list()
    cursor = -1
    followers = json.loads(call_followers_api(cursor,twitter_username))
    i = 0
    while(followers.has_key('errors') == True and str(followers['errors'][0]['message'])=='Rate limit exceeded'):
        print('Sleeping for 5 min...')
        time.sleep(300)
        followers = json.loads(call_followers_api(cursor,twitter_username))
        print(followers)
    if(followers.has_key('errors') == True):
        print(str(followers['errors'][0]['message']))
    if(followers.has_key('errors') == True and str(followers['errors'][0]['message'])=='Sorry, that page does not exist.'):
        print("err")
    if (followers.has_key("ids")):
        global total_followers_count 
        total_followers_count = len(followers['ids'])
        next_cursor = followers['next_cursor']
        for id in followers['ids']:
            followers_list.append(id)
            i=i+1
            if(i%1000==0):
                print(i)
        while(next_cursor!=0):
            followers = json.loads(call_followers_api(next_cursor,twitter_username))
            while(followers.has_key('errors') == True and str(followers['errors'][0]['message'])=='Rate limit exceeded'):
                print('Sleeping for 5 min...')
                time.sleep(300)
                followers = json.loads(call_followers_api(cursor,twitter_username))
                print(followers)
            next_cursor = followers['next_cursor']
            for id in followers['ids']:
                followers_list.append(id)
                i=i+1
                if(i%1000==0):
                    print(i)
    else:
        print("Profile private or non existant. Exit...")
        sys.exit()
    return followers_list

def map_followers(username):
    lats=[]
    longs=[]
    #xy=[]
    for user_id in (get_all_followers_ids(username)):
        #xy_pair=()
        user_info = json.loads(call_users_show_api_user_id(user_id))
        user_xy = geocode(user_info['location'])
        #xy_pair+=(user_xy.lat,user_xy.lng)
        #xy.append(xy_pair)
        if user_xy.lat is not None and user_xy.lng is not None:
            lats.append(user_xy.lat)
            longs.append(user_xy.lng)
    m = Basemap(projection='mill',llcrnrlat=-90,urcrnrlat=90,llcrnrlon=-180,urcrnrlon=180,resolution='c')
    m.drawcoastlines()
    m.drawcountries()
    m.drawmapboundary()
    x,y = m(longs, lats)
    m.hexbin(array(x), array(y), gridsize=30, mincnt=1, cmap='summer')
    m.colorbar(location='bottom')
    plt.title("Twitter Heatmap")
    plt.title("{} out of {} followers successfully geocoded of {}".format(len(lats),total_followers_count, username))
    plt.show()
#USAGE:
#map_followers("TwitterUserName")

