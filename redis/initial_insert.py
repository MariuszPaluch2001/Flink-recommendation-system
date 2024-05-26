import redis
import csv
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
with open('../data/in_db.edges') as fp:
    for row in csv.reader(fp):
        if float(row[2]) > 3.0:
            r.sadd('userProducts:' + row[0], row[1])
