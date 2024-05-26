import pymongo
import csv
with open('../data/in_db.edges') as fp:

    documents = [
        {
            "userID": row[0],
            "productID": row[1],
            "rating": row[2],
            "timestamp": row[3]
        } for row in csv.reader(fp)
    ]
mongo = pymongo.MongoClient("mongodb://localhost:27017/")

db = mongo["database"]
reviews = db["reviews"]
reviews.drop()
reviews.insert_many(documents)
