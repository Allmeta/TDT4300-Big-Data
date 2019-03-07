from pyspark import SparkContext, SparkConf
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
def findtuple(albumid,ar_cnt,alb_ar):
    for i in alb_ar:
        if albumid==i[0]:
            for j in ar_cnt:
                if i[1]==j[0]:
                    return j[1]
    return "NO COUNTRY"
def country_of_artist_in_album(topalbum,album,artist):
    #print(album.filter(lambda x: x["id"] in list(map((lambda x: x[0]), topalbum))).collect())
    al=album.filter(lambda x: x["id"] in map((lambda x: x[0]), topalbum)).map(lambda x: (x["id"],x["aid"])).collect()
    #print(al)
    ar=artist.filter(lambda x: x["id"] in map(lambda x: x[1], al)).map(lambda x: (x["id"], x["country"])).collect()
    print(ar)
    return(
            list(map((lambda x: (x[0],x[1],findtuple(x[0],ar,al))), topalbum)))
#print(country_of_artist_in_album(top_albums(albums),albums,artists))

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
                .map(lambda x: (x["id"], x["art"] or x["real"],x["country"]))
                .collect()           
    )
    nor_albums=(album
                .map(lambda x: (x["aid"], float(x["mtvc"])))
                .groupBy(lambda x: x[0])
                .mapValues(list)
                .map(lambda x: x[1])
                .flatMap(lambda x:x)
                .reduceByKey(lambda x: add(x)/len(x))
                #.join(nor_artists)
                .take(10)
                )
    return nor_albums
print(average_mtvc_norway_each_artist(artists,albums))