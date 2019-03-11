from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf=SparkConf().setAppName("main").setMaster("local")
sc=SparkContext(conf=conf)

albums=sc.textFile("albums.csv")
headera=['id','aid','title','genre','year','tracks','sales','rsc','mtvc','mmc']
albums=albums.map(lambda line: csv(line,headera))

artists=sc.textFile("artists.csv")
headerb=['id','real','art','role','year','country','city','email','zip']
artists=artists.map(lambda line: csv(line,headerb))

def write_to_tsv(rdd, name):
    with open('result{name}','w') as f:
        f.write('\t'.join(rdd))

def csv(line,head):
    l=line.split(",")
    return dict(zip(head,l))

#task1
def genres(rdd):
    return (rdd
            .map(lambda album: album["genre"])
            .distinct()
            .collect())
#print(genres(rdd))
#2
def oldest_artist(rdd):
    return (rdd
            .map(lambda artist: artist["year"])
            .sortBy(True)
            .first()
            )
#print(oldest_artist(artists))
#3
def artist_by_country(rdd):
    return (rdd
            .groupBy(lambda artist: artist["country"])
            .mapValues((list))
            .mapValues(len)
            .sortBy(lambda x: -x[1])
            .collect())
#print(artist_by_country(artists))
#4
def album_per_artist(rdd):
    return (rdd
            .groupBy(lambda a: a["id"])
            .mapValues(list)
            .mapValues(len)
            .sortBy(lambda x: int(x[0]))
            .sortBy(lambda x: -x[1])
            .collect())
#print(album_per_artist(albums))
#5
def sales_per_genre(rdd):
    return (rdd
            .map(lambda x: (x["genre"], int(x["sales"])))
            .groupBy(lambda x: x[0])
            .mapValues(list)
            .map(lambda x: x[1])
            .flatMap(lambda x: x)
            .reduceByKey(add)
            .sortBy(lambda x: x[0])
            .sortBy(lambda x: - int(x[1]))
            .collect())
#print(sales_per_genre(albums))
#6
def top_albums(rdd):
    return (rdd
            .map(lambda x: (x["aid"], (float(x["rsc"])+float(x["mtvc"])+float(x["mmc"]))/3))
            .takeOrdered(10, lambda x: -x[1])
            )
#print(top_albums(albums))
#7
def country_of_artist_in_album(album,artist):
    #print(album.filter(lambda x: x["id"] in list(map((lambda x: x[0]), topalbum))).collect())
    al=sc.parallelize(album
        .map(lambda x: (int(x["aid"]), (int(x["id"]), (float(x["rsc"])+float(x["mtvc"])+float(x["mmc"]))/3)))
        .takeOrdered(10, lambda x: -x[1][1])
        )
    ar=artist.map(lambda x: (int(x["id"]), x["country"]))
    return(al
        .join(ar)
        .map(lambda x: (x[1][0][0], x[1][0][1], x[1][1]))
        .collect())
#print(country_of_artist_in_album(albums,artists))

#8
#artists.joi
def highest_rated_artists(artist,album):
    rated_albums= (album
                    .filter(lambda x: float(x["mtvc"])==5.0)
                    .map(lambda x: x["aid"])
                    .collect()
    )
    #print(rated_albums)
    return (artist
            .filter(lambda x: x["id"] in rated_albums)
            .map(lambda x: ( x["art"]))
            .sortBy(lambda x: x)
            .collect()
            )
#print(highest_rated_artists(artists,albums))
#9
def average_mtvc_norway_each_artist(artist,album):
    nor_artists=(artist
        .filter(lambda x: x["country"]=="Norway")
        .map(lambda x: (int(x["id"]), x["art"]))         
    )
    albums=album.map(lambda x: (int(x["aid"]), float(x["mtvc"])))

    point=albums.join(nor_artists).groupByKey().mapValues(len)

    return(albums
        .join(nor_artists)
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]))
        .join(point)
        .map(lambda x: (x[1][0][1], "Norway", x[1][0][0]/x[1][1]))
        .collect()
        )
#print(average_mtvc_norway_each_artist(artists,albums))

# Task 10
SPARK=SparkSession.builder.config(conf=conf).getOrCreate()
ALBUM_DF=SPARK.createDataFrame(albums)
ARTISTS_DF=SPARK.createDataFrame(artists)

#a
def get_dist_art(df):
    return(df.agg(approx_count_distinct(df.id)
            .alias("distinct_artists"))
            .show())
#b
def get_dist_alb(df):
    return(df.agg(approx_count_distinct(df.id)
            .alias("distinct_albums"))
            .show())
#c
def get_dist_genre(df):
    return(df.agg(approx_count_distinct(df.genre)
            .alias("distinct_genres"))
            .show())
#d
def get_dist_country(df):
    return(df.agg(approx_count_distinct(df.country)
            .alias("distinct_countries"))
            .show())
#e
def min_year_of_pub(df):
    return(df.agg(min(df.year)
            .alias("min_year_of_pub"))
            .show())
#f
def max_year_of_pub(df):
    return(df.agg(max(df.year)
            .alias("max_year_of_pub"))
            .show())
#g
def min_year_of_birth(df):
    return(df.agg(min(df.year)
            .alias("min_year_of_birth"))
            .show())
#h
def max_year_of_birth(df):
    return(df.agg(max(df.year)
            .alias("max_year_of_birth"))
            .show())
def task_10():
    (get_dist_art(ARTISTS_DF))
    (get_dist_alb(ALBUM_DF))
    (get_dist_genre(ALBUM_DF))
    (get_dist_country(ARTISTS_DF))
    (min_year_of_pub(ALBUM_DF))
    (max_year_of_pub(ALBUM_DF))
    (min_year_of_birth(ARTISTS_DF))
    (max_year_of_birth(ARTISTS_DF))
#task_10()