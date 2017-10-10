import fiona
import pprint,os
from shapely.geometry import shape
from shapely.geometry import box
import psycopg2 as pc
import psycopg2.extras

conn = pc.connect("host=hostname port=port_# user=username password=pass dbname=hurricane")
#conn.set_isolation_level(pc.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
c1 = conn.cursor(cursor_factory=pc.extras.DictCursor)
c2 = conn.cursor()

with fiona.open('/home/scvetojevic/census_blocks/shapefiles/tabblock2010_46_pophu.shp') as src:
    src_schema=src.schema
    src_crs=src.crs
i=0

with fiona.open('/home/scvetojevic/twitter_places/census_blocks/shapefiles/metro_area_sentiment.shp') as metro_areas:
for file in os.listdir('/home/scvetojevic/twitter_places/census_blocks/shapefiles/'):
        if('shp' in file and 'xml' not in file and 'tabblock2010' in file):
            #print(file)
            census_blocks = fiona.open('/home/scvetojevic/twitter_places/census_blocks/shapefiles/{}'.format(file))
            for census_block in census_blocks:
                census_block_geom=shape(census_block['geometry'])
                print census_block_geom.centroid
                if ((census_block_geom.centroid).within(metro_area_geom)==True):
                    #output.write(census_block)
                    c2.execute("INSERT INTO census_blocks VALUES ('{STATEFP10}','{COUNTYFP10}','{TRACTCE10}','{BLOCKCE}','{BLOCKID10}',\
                            '{PARTFLG}','{HOUSING10}','{POP10}', ST_SetSRID(ST_GeometryFromText('{}'), 4326))"\
                            .format(census_block_geom.wkt,**(census_block['properties'])))
                i=i+1
                if(i%1000==0):
                   conn.commit()
            conn.commit()
