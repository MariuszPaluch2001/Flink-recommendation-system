import redis
import csv
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
with open('../data/in_db.edges') as fp:
    for row in csv.reader(fp):
        r.sadd('userRatings:' + row[0], row[1] + ":" +  row[2])
