import pathos.multiprocessing as mp
import psycopg2 as pc
import sys, datetime
import numpy as np
import ujson as json
from attrdict import AttrDict
import psycopg2.extras
from multiprocessing import Process
from multiprocessing import Pool

reload(sys)
sys.setdefaultencoding('utf-8')

class json_housekeeping(object):
    def __init__(self,dirty_dictionary):
        self.dict=dirty_dictionary
    @staticmethod    
    def stripped(x):
        return "".join([i for i in x if 31 < ord(i) < 127]) 
    def cleandict(self):
        l1 = len(self.dict)
        if not isinstance(self.dict, dict):
            return self.dict
        #print [k for k,v in self.dict.iteritems() if v is None]
        self.dict=dict((k,v) for k,v in self.dict.iteritems() if v is not None)

    def dict_utf2ascii(self):
        for a,b in self.dict.items():
            if(str(type(b)).find("unicode") != -1):
                b = self.stripped(b)
                self.dict[a] = b.encode('ascii','ignore')
    def dict_cleanup(self):
        for a,b in self.dict.items():
            if(str(b).find("\\u000")!=-1):
                self.dict[a] = b.replace("\\u000","")
            elif(str(type(b)).find("NoneType") != -1):
                self.dict[a] = "null"
        for a,b in self.dict.items():
            if(str(b).find("'") != -1):
                self.dict[a] = str(b).replace("'","''")
        #return self.dict
    @staticmethod      
    def tweet_source(src_html_str):
        from BeautifulSoup import BeautifulSoup
        return BeautifulSoup(src_html_str).find('a').getText()

class Tweet(object):
    total_obj_inst_time=datetime.datetime.now()-datetime.datetime.now()
    obj_count=0
    def __init__(self,tweet_str):
        t0=datetime.datetime.now()
        self.tweet_str=tweet_str
        self.tweet = json.loads(json.dumps(tweet_str))
        if self.tweet.has_key('coordinates')==True:
            self.coordinates=json.loads(json.dumps(self.tweet['coordinates']))
        self.tweet['user_id']=self.tweet['user']['id']
        self.tweet['place_id']=self.tweet['place']['id']
        self.hashtags_list=self.hashtags_list()
        self.urls_list=self.urls_list()
        self.media_list=self.media_list()
        self.uml=self.user_mentions_list()

        j = json_housekeeping(self.tweet)
        j.dict_cleanup()
        self.tweet = j.dict
        Tweet.total_obj_inst_time+=(datetime.datetime.now()-t0)
        Tweet.obj_count+=1
        #print "hashtags: {}, urls: {}, media: {}, user_mentions: {}".format(len(self.hashtags_list),len(self.urls_list),len(self.media_list),len(self.uml))


    def list_attributes(self):
        for key, val in self.tweet.iteritems():
            print(key.encode('ascii','ignore'), type(val))
    def return_attribute(self,attr):
        if self.tweet.has_key(attr):
            return self.tweet.get(attr)
        else:
            pass
    def urls_list(self):
        if (self.tweet['entities']).has_key('urls')==True and len(self.tweet['entities']['urls'])>0:
            return [urls for urls in self.tweet['entities']['urls']] #list of dictionaries
        else: return []
    
    def media_list(self):
        if (self.tweet['entities']).has_key('media')==True and len(self.tweet['entities']['media'])>0:
            return [media for media in self.tweet['entities']['media']]
        else: return []

    
    def user_mentions_list(self):
        if (self.tweet['entities']).has_key('user_mentions')==True and len(self.tweet['entities']['user_mentions'])>0:
            return [uml for uml in self.tweet['entities']['user_mentions']]
        else: return []

    def hashtags_list(self):
        return [hashtag for hashtag in self.tweet['entities']['hashtags'] if len(self.tweet['entities']['hashtags'])>0]
    
    @property
    def data_dict(self):
        return AttrDict(self.tweet)


class FirstDegreeDict(Tweet):
    total_obj_inst_time=datetime.datetime.now()-datetime.datetime.now()
    obj_count=0
    def __init__(self,tweet_str,key_str):
        super(FirstDegreeDict,self).__init__(tweet_str)
        t0=datetime.datetime.now()
        self.tweet_str=tweet_str
        self.key_str=key_str
        #if self.key_str in ["user","place"]:
        self.obj = (json.loads(json.dumps(self.tweet_str)))[self.key_str] #in this case self.obj will is a dictionary
        if self.obj.has_key('bounding_box')==True:
            self.obj['bounding_box']=json.dumps(self.obj['bounding_box'])
            #print json.dumps(self.obj['bounding_box'])
        j = json_housekeeping(self.obj)
        j.dict_cleanup()
        self.obj=j.dict
        self.obj=j.dict
        #else:
        #    print "PROCESSING HASHTAGS"
        #    self.obj = (json.loads(json.dumps(self.tweet_str)))['entities'][self.key_str] #in this case self.obj a list of dictionaries 
        ###this is why cursor insert in sef.chunk_processing fucnttion needs additional loop through list of dictionaries in the latter case    

        FirstDegreeDict.total_obj_inst_time+=(datetime.datetime.now()-t0)
        FirstDegreeDict.obj_count+=1
    def list_attributes(self):
        super(FirstDegreeDict, self).list_attributes()
    def return_attribute(self,attr):
        super(FirstDegreeDict, self).return_attribute(attrdict)
    @property
    def data_dict(self):
        return AttrDict(self.obj)

class TweetJson2RelDB(object):
    def __init__(self,conn_a,conn_b,table_a,table_b,db_b,chunk_length,id_col,tweet_col,ClassName,key_str="user"): #note that connection string must always include the dbname
        self.conn_a=conn_a
        self.conn_b=conn_b
        self.table_a=table_a
        self.table_b=table_b
        self.ClassName=ClassName
        self.key_str=key_str
        self.chunk_length=chunk_length
        self.db_b=db_b
        self.id_col=id_col
        self.tweet_col=tweet_col
        self.ids=self.ids_fetch()
        self.existing_users_list=self.list_existing_users()
        if self.ClassName.__name__=="Tweet":
            self.unique_users_ind = self.eliminate_existing_tweet_ids()
        else:
            self.unique_users_ind = self.list_unique()     
        self.chunks_of_ids=np.array(self.id_batches())
        self.cols_definitions=self.cols_definitions()

    def list_existing_users(self):
        t0 = datetime.datetime.now()
        conn_obj_b = pc.connect(self.conn_b)
        c=conn_obj_b.cursor(cursor_factory=pc.extras.DictCursor)
        c.execute("select distinct id from {} order by 1 asc;".format(self.table_b))
        user_ids = []
        for a in c:
            user_ids.append(a['id'])
        try:
            conn_obj_b.close()
        except:
            pass
        print "Existing {}s fetched in {}".format(self.key_str,datetime.datetime.now()-t0)
        return user_ids

    def ids_fetch(self):
        t0 = datetime.datetime.now()
        conn_obj_a = pc.connect(self.conn_a)
        c=conn_obj_a.cursor(cursor_factory=pc.extras.DictCursor)
        sql = "select id from {0} order by 1 asc limit 1000000;".format(self.table_a)
        c.execute(sql)
        ids=[]
        for row in c:
            ids.append(row[0])
        try:
            conn_obj_a.close()
        except:
            pass
        print "{} IDs selected in {}".format(len(ids),datetime.datetime.now()-t0)
        return ids
    def eliminate_existing_tweet_ids(self):
        t0 = datetime.datetime.now()
        #conn_obj_a = pc.connect(self.conn_a)
        conn_obj_b = pc.connect(self.conn_b)
        c_b=conn_obj_b.cursor(cursor_factory=pc.extras.DictCursor)
        sql="select id,tweet from {0} where {1} = any(array[{2}]::integer[]) order by 1 asc;\
            ".format(self.table_a,self.id_col,", ".join(str(e).replace('(','').replace(')','').replace(',','')for e in self.ids))
        #print(sql)
        c_b.execute(sql)
        #c_b=conn_obj_b.cursor(cursor_factory=pc.extras.DictCursor)
        #c_b.execute("select id from {} order by 1 asc;".format(self.table_b))
        #existing_tweet_id_ids = c_b.fetchall()
        ids_for_tweet_ids_not_already_in_destination_table=[]
        for row in c_b:
            tweet=self.ClassName(row[self.tweet_col])
            if tweet.data_dict.id not in self.existing_users_list:
                ids_for_tweet_ids_not_already_in_destination_table.append(row['id'])
        try:
            conn_obj_b.close()
        except:
            pass
        print "Existing {}s fetched in {}".format(self.key_str,datetime.datetime.now()-t0)
        return ids_for_tweet_ids_not_already_in_destination_table

    def list_unique(self):
        t0 = datetime.datetime.now()
        conn_obj_a = pc.connect(self.conn_a)
        c=conn_obj_a.cursor(cursor_factory=pc.extras.DictCursor)
        sql = "select {1},{3} from {0} where {1} = any(array[{2}]::integer[])\
         order by 1 asc;".format(self.table_a,self.id_col,", ".join(str(e).replace('(','').replace(')','').replace(',','')\
          for e in self.ids),self.tweet_col)
        c.execute(sql)
        query_places={} #dict that will have user_id as keys only id of their first occurence as value
        print "Unique {}_ids and tweets fetched for unique user_id filtering in {}".format(self.key_str,datetime.datetime.now()-t0)
        t1=datetime.datetime.now()
        for row in c:
            #print row[self.tweet_col]
            try:
                p=self.ClassName(row[self.tweet_col],self.key_str)
                if p.data_dict.id not in self.existing_users_list and query_places.has_key(p.data_dict.id)==False:
                    query_places[p.data_dict.id]=row[self.id_col]
                else:
                    pass
            except Exception as e:
                print "ID: {}; Error: {}".format(row[self.tweet_col]['id'],e.message)
                pass
        try:
            conn_obj_a.close()
        except:
            pass
        if self.ClassName.obj_count>0:
            print "average {} object instantioation time: {}".format(self.key_str,datetime.timedelta(seconds=self.ClassName.total_obj_inst_time.total_seconds()/self.ClassName.obj_count))       
        else:
            print self.ClassName.obj_count
        #print("users initially: {}".format(query_users)) # test ===>>> works like a charm, just like row_number() in  sql does...
        print "Unique {}_ids filtered {}".format(self.key_str,datetime.datetime.now()-t1)
        #return sorted([a for a in query_users.values()])
        return [a for a in query_places.values()] #returns table ids where user_id, place_id etc... occur for the first time

    def id_batches(self):
        t0 = datetime.datetime.now()
        new_l=[]
        i=0
        while i < len(self.unique_users_ind): #ranije bilo unique_users_ind
            new_l.append(self.unique_users_ind[i:i+self.chunk_length]) #i ovde isto slicovana pogresna lista
            i+=self.chunk_length
        print "List of ids split to batches in {}".format(datetime.datetime.now()-t0)
        return new_l

    #this function grabs column names and their respective data types from the destination database table
    #columns MUST be named as tweet json keys e.g. table users has to have columns id, screen_name etc...
    def cols_definitions(self):
        t0 = datetime.datetime.now()
        conn_obj_b = pc.connect(self.conn_b)
        cols_cur = conn_obj_b.cursor(cursor_factory=pc.extras.DictCursor)
        cols_cur.execute("select column_name,data_type from information_schema.columns\
                          where table_catalog = '{}' and table_name = '{}';".format(self.db_b,self.table_b))
        db_cols_def = ""
        for col in cols_cur:
            if str(col["data_type"]) in ['character varying','character']:
                db_cols_def+="'{{{0}}}',".format(str(col["column_name"]))
            elif str(col["data_type"]) in ['timestamp with time zone','timestamp without time zone','timestamp','datetime','date']:
                db_cols_def+="'{{{0}}}'::{1},".format(str(col["column_name"]),str(col["data_type"]))
            elif str(col['column_name']) in ['bounding_box','geo','coordinates']:
                db_cols_def+="ST_SetSRID(ST_GeomFromGeoJSON('{{{0}}}'), 4326),".format(str(col['column_name']))
            else:
                db_cols_def+="{{{0}}},".format(str(col["column_name"]))
        try:
            conn_obj_b.close()
        except:
            pass
        print "Column definitions fetched from the db table in {}".format(datetime.datetime.now()-t0)
        return db_cols_def[0:len(db_cols_def)-1]   #strip the trailing comma

    def entities_processing(self,*args):     ###for this function *args is going to be the sublist of table ids...
        conn_obj_a=pc.connect(self.conn_a)
        conn_obj_b=pc.connect(self.conn_b)

        conn_obj_b.set_isolation_level(pc.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        c_a=conn_obj_a.cursor(cursor_factory=pc.extras.DictCursor)
        c_h=conn_obj_b.cursor()
        c_m=conn_obj_b.cursor()
        c_u=conn_obj_b.cursor()
        c_um=conn_obj_b.cursor()
        c_xy=conn_obj_b.cursor()
        #print self.cols_definitions
        try:
            if len(args)==0:
                print("No more records")
                sys.exit()
            sql = "select {2},{0} from {1} where {2} = any(array[{3}]::integer[]) and length({0}::varchar) > 400;\
                ".format(self.tweet_col,self.table_a,self.id_col, ", ".join(str(e).replace('(','').replace(')','').replace(',','')\
                 for e in args))
            c_a.execute(sql)
            result_set=c_a.fetchall()
            for row in result_set:
                try:
                    tweet=Tweet(row['tweet'])
                    #hashtags
                    for hashtag in tweet.hashtags_list:
                        #print hashtag
                        hashtag['id']=tweet.data_dict.id
                        hashtag['text']=((hashtag['text'].encode('ascii','ignore')).replace('"','')).replace("'","''")
                        sql=("insert into {0} values ({1});".format("hashtags","{id},'{text}'")).format("hashtags",**hashtag)
                        #print sql
                        c_h.execute(sql)
                    #media
                    for medium in tweet.media_list:
                        medium['id']=tweet.data_dict.id
                        sql=("insert into {0} values ({1});".format("media","{id},'{type}','{expanded_url}'")).format("media",**medium)
                        c_m.execute(sql)
                    #urls
                    for url in tweet.urls_list:
                        #print hashtag
                        url['id']=tweet.data_dict.id
                        sql=("insert into {0} values ({1});".format("urls","{id},'{expanded_url}'")).format("urls",**url)
                        c_u.execute(sql)
                    #user mentions
                    for um in tweet.uml:
                        um['id']=tweet.data_dict.id
                        um['name']=((um['name'].encode('ascii','ignore')).replace('"','')).replace("'","''")
                        um['screen_name']=((um['screen_name'].encode('ascii','ignore')).replace('"','')).replace("'","''")
                        sql=("insert into {0} values ({1});".format("user_mentions","{id},'{name}','{screen_name}'")).format("user_mentions",**um)
                        c_um.execute(sql)
                    xy={}
                    if tweet.coordinates is not None:
                        xy['id']=tweet.data_dict.id
                        xy['geo']=json.dumps(tweet.coordinates)
                        sql=("insert into {0} values ({1});".format("coordinates","{id},ST_SetSRID(ST_GeomFromGeoJSON('{geo}'), 4326)")).format("coordinates",**xy)
                        c_xy.execute(sql)

                except Exception as e:
                    print "Error: {}".format(e.message)
                #except pc.Error as e:
                #    print "Error: {}".format(e.message)
            conn_obj_b.commit()
            conn_obj_b.close()
            conn_obj_a.close()
            print "Thread done. Exiting...".format()
        except pc.Error as e:
            print "Error: {}, SQL: {}".format(e.message,sql)

    def chunk_processing(self,*args):     ###for this function *args is going to be the sublist of table ids...
        conn_obj_a=pc.connect(self.conn_a)
        conn_obj_b=pc.connect(self.conn_b)

        conn_obj_b.set_isolation_level(pc.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        c_a=conn_obj_a.cursor(cursor_factory=pc.extras.DictCursor)
        c_b=conn_obj_b.cursor()
        #print self.cols_definitions
        try:
            if len(args)==0:
                print("No more records")
                sys.exit()
            sql = "select {2},{0} from {1} where {2} = any(array[{3}]::integer[]) and length({0}::varchar) > 400;\
                ".format(self.tweet_col,self.table_a,self.id_col, ", ".join(str(e).replace('(','').replace(')','').replace(',','')\
                 for e in args))
            c_a.execute(sql)
            result_set=c_a.fetchall()
            for row in result_set:
                try:
                    if self.ClassName.__name__=="Tweet":
                        a=Tweet(row[self.tweet_col])
                    else:
                        a=FirstDegreeDict(row[self.tweet_col],self.key_str)
                    sql=("insert into {0} values ({1});".format(self.table_b,self.cols_definitions)).format(self.table_b,**a.data_dict)
                    #print(sql)
                    c_b.execute(sql)
                except Exception as e:
                    print "Error: {}".format(e.message)
                #except pc.Error as e:
                #    print "Error: {}".format(e.message)
            conn_obj_b.commit()
            conn_obj_b.close()
            conn_obj_a.close()
            print "Thread done. Exiting...".format()
        except pc.Error as e:
            print "Error: {}, SQL: {}".format(e.message,sql)
    def entities_insert(self):
        print "\n\n\n Processing {}s".format(self.key_str)
        if len(self.unique_users_ind)>1:
            cores = (mp.cpu_count()-1)
            pool = mp.Pool(cores)
            for i in range(0,len(self.chunks_of_ids)):
                jobs=pool.apply_async(self.entities_processing, (self.chunks_of_ids[i])) #*args in the function signiture eliminates wrong number of arguments error
                print "{}-th threads: {}".format(i, len(self.chunks_of_ids[i]))
            pool.close()
            pool.join()
            jobs.get()

    def parallel_insert(self):
        print "\n\n\n Processing {}s".format(self.key_str)
        if len(self.unique_users_ind)>1:
            cores = (mp.cpu_count()-1)
            pool = mp.Pool(cores)
            for i in range(0,len(self.chunks_of_ids)):
                jobs=pool.apply_async(self.chunk_processing, (self.chunks_of_ids[i])) #*args in the function signiture eliminates wrong number of arguments error
                print "{}-th threads: {}".format(i, len(self.chunks_of_ids[i]))
            pool.close()
            pool.join()
            jobs.get()
        else:
            print "No new {}s".format(self.key_str)

def main():
    t0=datetime.datetime.now()
    conn_a="host=YourHost user=YourUser password=YourPass port=YourPort dbname=YourDB"
    conn_b="host=YourHost user=YourUser password=YourPass port=YourPort dbname=YourDB"
    db_b = "tgs7_new"
    table_a="json_tweets"
    table_b = "places"
    chunk_length= 1000
    place = TweetJson2RelDB(conn_a,conn_b,table_a,table_b,db_b,chunk_length,"id","tweet",FirstDegreeDict,"place")
    table_b = "users"
    user = TweetJson2RelDB(conn_a,conn_b,table_a,table_b,db_b,chunk_length,"id","tweet",FirstDegreeDict,"user")
    table_b = "tweets"
    tweet = TweetJson2RelDB(conn_a,conn_b,table_a,table_b,db_b,chunk_length,"id","tweet",Tweet)
    
    hashtags = TweetJson2RelDB(conn_a,conn_b,table_a,table_b,db_b,chunk_length,"id","tweet",Tweet,)
    #pool = Pool(processes=3)

    #result_places = pool.map_async(place.parallel_insert(),())
    #result_users = pool.map_async(user.parallel_insert(),())
    #result_tweets = pool.map_async(tweet.parallel_insert(),())
    
    p1 = Process(target=place.parallel_insert())
    p1.start()
    p2 = Process(target=user.parallel_insert())
    p2.start()
    p3 = Process(target=tweet.parallel_insert())
    p3.start()
    p4 = Process(target=hashtags.entities_insert())
    p4.start()

if __name__=="__main__":
    main()
