import psycopg2 as pc 
import base64 
import json 
import time 
import sys 
import psycopg2.extras
import BeautifulSoup as bs
import psycopg2.extensions
#UTF8 insert
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

def main():
    def cleandict(d):
        if not isinstance(d, dict):
            return d
        return dict((k,cleandict(v)) for k,v in d.iteritems() if v is not None)
    #try:
    #log_file_name = 'error_log.txt'
    start_t = time.time()
    databases = ['twitter_geo_split1','twitter_geo_split2','twitter_geo_split3','twitter_geo_split4','twitter_geo_split5','twitter_geo_split6']
    tables = ['tweets_stream_nw_i','tweets_stream_nw_ii','tweets_stream_nw_iii','tweets_stream_se','tweets_stream_sw']
    for db in databases:
        for table in tables:
            #log = open(log_file_name,'a')
            print('Db: %s table: %s \n' %(str(db),str(table)))
            #log.close()
            con1 = pc.connect("host=source_db_host user=user password=pass dbname=%s"%(db))
            con2 = pc.connect("host=desination_db_host port=5433 user=user password=pass dbname=db_name")
            con2.set_client_encoding('UTF8')
            c0 = con2.cursor(cursor_factory=psycopg2.extras.DictCursor)
            c1 = con1.cursor(cursor_factory=psycopg2.extras.DictCursor)
            c2 = con1.cursor(cursor_factory=psycopg2.extras.DictCursor)
            c3 = con2.cursor()
            id_load_start = time.time()
            if(table=='tweets_stream_ne_i'):
                region_id=1
            elif(table=='tweets_stream_ne_ii'):
                region_id=2
            elif(table=='tweets_stream_nw_i'):
                region_id=3
            elif(table=='tweets_stream_nw_ii'):
                region_id=4
            elif(table=='tweets_stream_nw_iii'):
                region_id=5
            elif(table=='tweets_stream_se'):
                region_id=6
            elif(table=='tweets_stream_sw'):
                region_id=7
            if(db=='twitter_geo_split'):
                db_index=0
            else:
                db_index=db[len(db)-1:]
            max_id=0
            print("Database: {0}, table: {1}...".format(db,table))
            c0.execute("SELECT ID, REGION_LOCAL_ID FROM tweets_5_regions WHERE DB={0} AND REGION_ID={1} ORDER BY 1 DESC;".format(db_index,region_id)) 
            for a in c0:
                max_id=a[1]
            print(max_id)
            c1.execute("SELECT id FROM {0} WHERE ID > {1} ORDER BY 1 ASC;".format(table,max_id))
            # point = ogr.Geometry(ogr.wkbPoint)
            parsing_start = time.time()
            print("IDs loaded...")
            x = 0
            while True:
                id_list = c1.fetchmany(10000)
                if len(id_list)==0:
                    print("No more records")
                    break
                c3.execute("SAVEPOINT s1;")
                try:
                    c2.execute("select * from %s where id = any(array[%s]::integer[]);"% (table, ", ".join(str(e).replace('(','').replace(')','').replace(',','') for e in id_list)))
                    for row in c2:
                        if(len(base64.b64decode(row[2]))>200):
                            json_o = json.loads(base64.b64decode(row[2]))
                            if((json_o.has_key('coordinates') == True) and (type(json_o['coordinates']) is dict) and json_o.has_key('source')==True):
                                json_data = dict(
                                tweet_id = json_o['id'],
                                created_at = json_o['created_at'],
                                tweet = row[2],
                                x=json_o['coordinates']['coordinates'][0],
                                y=json_o['coordinates']['coordinates'][1],
                                user_id = json_o['user']['id'],
                                source=((bs.BeautifulSoup(str(json_o['source']))).findAll('a')[0]).contents[0]
                                )
                                unicode_dict_cleanup(json_data)
                                tweets_columns = ('DB','REGION_ID', 'REGION_LOCAL_ID', 'TWEET_ID', 'XY', 'TWEET','CREATED_AT', 'USER_ID','SOURCE')
                                #tweets_placeholders = "%s::int,%s::int,'%s'::int,'%s'::bigint,st_setsrid(st_makepoint('%s','%s'),4326),replace(convert_from(decode('%s','base64'),'UTF8'),'\u0000','')::jsonb,'%s'::timestamp,'%s'::bigint, substring('%s',position('>' in '%s')+1,position('</' in '%s')-position('>' in '%s')-1)" % (db_index,region_id,str(row[0]),str(json_data['tweet_id']),str(json_data['x']),str(json_data['y']),str(json_data['tweet']),str(json_data['created_at']),str(json_data['user_id']),str(json_data['source']),str(json_data['source']),str(json_data['source']),str(json_data['source']))
                                tweets_placeholders = "%s::int,%s::int,'%s'::int,'%s'::bigint,st_setsrid(st_makepoint('%s','%s'),4326),replace(convert_from(decode('%s','base64'),'UTF8'),'\u0000','')::jsonb,'%s'::timestamp,'%s'::bigint, '%s'" % (db_index,region_id,str(row[0]),str(json_data['tweet_id']),str(json_data['x']),str(json_data['y']),str(json_data['tweet']),str(json_data['created_at']),str(json_data['user_id']),str(json_data['source']))
                                #tweets_placeholders="{0}, 
                                #{1},'{2}'::bigint,st_setsrid(st_makepoint('{x}','{y}'),4326),replace(convert_from(decode('{tweet}','base64'),'UTF8'),'\u0000','')::jsonb,'{created_at}'::timestamp".format(db_index,region_id,str(row[0]),**json_data)
                                sql_tweets_insert = 'INSERT INTO tweets_5_regions (%s, %s,%s,%s,%s,%s,%s,%s,%s) VALUES (%s)' % (tweets_columns[0],tweets_columns[1],tweets_columns[2],tweets_columns[3],tweets_columns[4],tweets_columns[5],tweets_columns[6],tweets_columns[7],tweets_columns[8], tweets_placeholders)
                                #sql_tweets_insert = "INSERT INTO TWEETS ({0},{1},{2},{3},{4},{5},{6}) VALUES ({7})".format(*tweets_columns, tweets_placeholders)
                                #print(sql_tweets_insert)
                                x=x+1
                                c3.execute(sql_tweets_insert)
                                #c3.callproc('tweets_insert',[tweets_placeholders])
                    con2.commit()
                    print("Rows inserted: {0}".format(x))
                except Exception, e:
                    c3.execute("ROLLBACK TO SAVEPOINT s1;")
                    print 'Error {0}'.format(e)
                    print("Connection2 rollback")
                    for a in id_list:
                        try:
                            #print(a[0])
                            con3 = pc.connect("host=source_db_host user=user password=pass dbname=%s"%(db))
                            con4 = pc.connect("host=desination_db_host port=5433 user=user password=pass dbname=db_name")
                            con4.set_client_encoding('UTF8')
                            c4 = con3.cursor()
                            c5 = con4.cursor()
                            c4.execute("select * from %s where id = %s::int;"% (table, a[0]))
                            for row in c4:
                                if(len(base64.b64decode(row[2]))>200):
                                    json_o = json.loads(base64.b64decode(row[2]))
                                    if((json_o.has_key('coordinates') == True) and (type(json_o['coordinates']) is dict)):
                                        json_data = dict(
                                        tweet_id = json_o['id'],
                                        created_at = json_o['created_at'],
                                        tweet = row[2],
                                        x=json_o['coordinates']['coordinates'][0],
                                        y=json_o['coordinates']['coordinates'][1],
                                        user_id = json_o['user']['id'],
                                        source=((bs.BeautifulSoup(str(json_o['source']))).findAll('a')[0]).contents[0]
                                        )
                                        x=x+1
                                        unicode_dict_cleanup(json_data)
                                        tweets_columns = ('DB','REGION_ID', 'REGION_LOCAL_ID', 'TWEET_ID', 'XY', 'TWEET','CREATED_AT', 'USER_ID','SOURCE')
                                        #tweets_placeholders = "%s::int,%s::int,'%s'::int,'%s'::bigint,st_setsrid(st_makepoint('%s','%s'),4326),replace(convert_from(decode('%s','base64'),'UTF8'),'\u0000','')::jsonb,'%s'::timestamp,'%s'::bigint, substring('%s',position('>' in '%s')+1,position('</' in '%s')-position('>' in '%s')-1)" % (db_index,region_id,str(row[0]),str(json_data['tweet_id']),str(json_data['x']),str(json_data['y']),str(json_data['tweet']),str(json_data['created_at']),str(json_data['user_id']),str(json_data['source']),str(json_data['source']),str(json_data['source']),str(json_data['source']))
                                        tweets_placeholders = "%s::int,%s::int,'%s'::int,'%s'::bigint,st_setsrid(st_makepoint('%s','%s'),4326),replace(convert_from(decode('%s','base64'),'UTF8'),'\u0000','')::jsonb,'%s'::timestamp,'%s'::bigint, '%s'" % (db_index,region_id,str(row[0]),str(json_data['tweet_id']),str(json_data['x']),str(json_data['y']),str(json_data['tweet']),str(json_data['created_at']),str(json_data['user_id']),str(json_data['source']))
                                        #tweets_placeholders="{0}, 
                                        #{1},'{2}'::bigint,st_setsrid(st_makepoint('{x}','{y}'),4326),replace(convert_from(decode('{tweet}','base64'),'UTF8'),'\u0000','')::jsonb,'{created_at}'::timestamp".format(db_index,region_id,str(row[0]),**json_data)
                                        sql_tweets_insert = 'INSERT INTO tweets_5_regions (%s, %s,%s,%s,%s,%s,%s,%s,%s) VALUES (%s)' % (tweets_columns[0],tweets_columns[1],tweets_columns[2],tweets_columns[3],tweets_columns[4],tweets_columns[5],tweets_columns[6],tweets_columns[7],tweets_columns[8], tweets_placeholders)
                                        c5.execute(sql_tweets_insert)
                                        con4.commit()
                            print("Rows inserted: {0}".format(x))
                        except Exception, exc:
                            print("%s: %s" % (exc.__class__.__name__, exc))
                            pass
                        finally:
                            if con3:
                                con3.close()
                            if con4:
                                con4.close()
                    c2.execute("RELEASE SAVEPOINT s1;")
                    pass
                    #sys.exit(1)
                except IOError, e:
                    print 'Error %s' % e
                    #sys.exit(1)
                #id_list = c1.fetchmany(10000) finally:
            print('Total elapsed time: %s Total records processed: %s \n' %(str(round(time.time() - start_t,2)),str(x)))
            if con1:
                con1.close()
            if con2:
                con2.close()
            
def stripped(x):
    return "".join([i for i in x if 31 < ord(i) < 127]) 
def unicode_dict_cleanup(json_dictionary):
    for a,b in json_dictionary.items():
        if(str(type(b)).find("unicode") != -1):
            b = stripped(b)
            json_dictionary[a] = b.encode('utf-8')
        elif(str(b).find("\\u000")!=-1):
            json_dictionary[a] = b.replace("\\u000","")
        elif(str(type(b)).find("NoneType") != -1):
            json_dictionary[a] = "null"
    for a,b in json_dictionary.items():
        if(str(b).find("'") != -1):
            json_dictionary[a] = str(b).replace("'","''") 
if __name__ == "__main__": main()
