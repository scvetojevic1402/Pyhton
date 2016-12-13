import pathos.multiprocessing as mp
import psycopg2 as pc
import sys, datetime
import numpy as np
import ujson as json
from attrdict import AttrDict
import psycopg2.extras
from BeautifulSoup import BeautifulSoup
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
        print [k for k,v in self.dict.iteritems() if v is None]
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
        return BeautifulSoup(src_html_str).find('a').getText()

class Tweet(object):
    total_obj_inst_time=datetime.datetime.now()-datetime.datetime.now()
    obj_count=0
    def __init__(self,tweet_str):
        t0=datetime.datetime.now()
        self.tweet_str=tweet_str
        self.tweet = json.loads(json.dumps(tweet_str))
        j = json_housekeeping(self.tweet)
        j.dict_cleanup()
        self.tweet = j.dict
        Tweet.total_obj_inst_time+=(datetime.datetime.now()-t0)
        Tweet.obj_count+=1
    def list_attributes(self):
        for key, val in self.tweet.iteritems():
            print(key.encode('ascii','ignore'), type(val))
    def return_attribute(self,attr):
        if self.tweet.has_key(attr):
            return self.tweet.get(attr)
        else:
            pass
    def hashtags_list(self):
        return [json.dumps(hashtag['text']) for hashtag in self.tweet['entities']['hashtags'] if len(self.tweet['entities']['hashtags'])>0]
    @property
    def tweet_dict(self):
        return AttrDict(self.tweet)

class FirstDegreeDict(Tweet):
    total_obj_inst_time=datetime.datetime.now()-datetime.datetime.now()
    obj_count=0
    def __init__(self,tweet_str,key_str):
        super(FirstDegreeDict,self).__init__(tweet_str)
        t0=datetime.datetime.now()
        self.tweet_str=tweet_str
        self.key_str=key_str
        self.obj = (json.loads(json.dumps(self.tweet_str)))[self.key_str]
        j = json_housekeeping(self.obj)
        j.dict_cleanup()
        self.obj=j.dict
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
    def __init__(self,conn_a,conn_b,table_a,table_b,db_b,chunk_length,id_col,tweet_col,ClassName,key_str): 
        #note that connection string must always include the dbname
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
        sql = "select id from {0} order by 1 asc limit 100;".format(self.table_a)
        c.execute(sql)
        ids=[]
        for row in c:
            ids.append(row[0])
        try:
            conn_obj_a.close()
        except:
            pass
        print "IDs selected in {}".format(datetime.datetime.now()-t0)
        return ids

    def list_unique(self):
        t0 = datetime.datetime.now()
        conn_obj_a = pc.connect(self.conn_a)
        c=conn_obj_a.cursor(cursor_factory=pc.extras.DictCursor)
        sql = "select {4},{3} from {0} where {1} = any(array[{2}]::integer[])\
         order by 1 asc;".format(self.table_a,self.id_col,", ".join(str(e).replace('(','').replace(')','').replace(',','')\
          for e in self.ids),self.tweet_col,self.id_col)
        #print sql
        c.execute(sql)
        query_places={} #dict that will have user_id as keys only id of their first occurence as value
        print "Unique {}_ids and tweets fetched for unique user_id filtering in {}".format(self.key_str,datetime.datetime.now()-t0)
        t1=datetime.datetime.now()
        for row in c:
            #print row[self.tweet_col]
            p=self.ClassName(row[self.tweet_col],self.key_str)
            #print [a for a in p.data_dict.keys()]
            if p.data_dict.id not in self.existing_users_list and query_places.has_key(p.data_dict.id)==False:
                query_places[p.data_dict.id]=row[self.id_col]
            else:
                pass
        if self.ClassName.obj_count>0:
            print "average {} object instantioation time: {}".format(self.key_str,datetime.timedelta(seconds=self.ClassName.total_obj_inst_time.total_seconds()/self.ClassName.obj_count))       
        else:
            print self.ClassName.obj_count
        #print("users initially: {}".format(query_users)) # test ===>>> works like a charm, just like row_number() in  sql does...
        print "Unique {}_ids filtered {}".format(self.key_str,datetime.datetime.now()-t1)
        #return sorted([a for a in query_users.values()])
        return [a for a in query_places.values()]

    def id_batches(self):
        t0 = datetime.datetime.now()
        new_l=[]
        i=0
        while i < len(self.unique_users_ind):
            new_l.append(self.unique_users_ind[i:i+self.chunk_length])
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
            else:
                db_cols_def+="{{{0}}},".format(str(col["column_name"]))
        try:
            conn_obj_b.close()
        except:
            pass
        print "Column definitions fetched from the db table in {}".format(datetime.datetime.now()-t0)
        #print(db_cols_def)
        return db_cols_def[0:len(db_cols_def)-1]   #strip the trailing comma


    def chunk_processing(self,*args):     ###for this function *args is going to be the sublist of table ids...
        conn_obj_a=pc.connect(self.conn_a)
        conn_obj_b=pc.connect(self.conn_b)

        conn_obj_b.set_isolation_level(pc.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        c_a=conn_obj_a.cursor(cursor_factory=pc.extras.DictCursor)
        c_b=conn_obj_b.cursor()
        db_cols_def = self.cols_definitions
        try:
            if len(args)==0:
                print("No more records")
                sys.exit()
            sql = "select {2},{0} from {1} where {2} = any(array[{3}]::integer[]);\
                ".format(self.tweet_col,self.table_a,self.id_col, ", ".join(str(e).replace('(','').replace(')','').replace(',','')\
                 for e in args))
            c_a.execute(sql)
            result_set=c_a.fetchall()
            for row in result_set:
                a=FirstDegreeDict(row[self.tweet_col],self.key_str)
                c_b.execute(("insert into {0} values ({1});".format(self.table_b,db_cols_def)).format(self.table_b,**a.data_dict))
            conn_obj_b.commit()
            conn_obj_b.close()
            conn_obj_a.close()
        except pc.Error as e:
            print "Error: {}".format(e.message)

    def parallel_insert(self):
        t0=datetime.datetime.now()
        if len(self.unique_users_ind)>1:
            cores = (mp.cpu_count()-1)
            pool = mp.Pool(cores)
            for i in range(0,len(self.chunks_of_ids)):
                jobs = pool.apply_async(self.chunk_processing, (self.chunks_of_ids[i])) #*args in the function signiture eliminates wrong number of arguments error
                print "{}-th threads: {}".format(i, len(self.chunks_of_ids[i]))
            pool.close()
            pool.join()
            jobs.get()
        else:
            print "No new users"
        print datetime.datetime.now() - t0

def main():
    conn_a="host=YourHost user=YourUser password=YourPass port=YourPort dbname=YourDB"
    conn_b="host=YourHost user=YourUser password=YourPass port=YourPort dbname=YourDB"
    db_b = "tgs7_new"
    table_a="paris_all"
    table_b = "json2relational_places"
    chunk_length= 10
    tweet_place = TweetJson2RelDB(conn_a,conn_b,table_a,table_b,db_b,chunk_length,"id","tweet",FirstDegreeDict,"place")
    tweet_place.parallel_insert()
    tweet_user = TweetJson2RelDB(conn_a,conn_b,table_a,table_b,db_b,chunk_length,"id","tweet",FirstDegreeDict,"user")
    tweet_user.parallel_insert()

if __name__=="__main__":
    main()
