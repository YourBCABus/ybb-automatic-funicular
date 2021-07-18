import os
import redis
import json
from pymongo import MongoClient
from bson.objectid import ObjectId

def main():
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )

    db = MongoClient(os.getenv("DATABASE_URL")).yourbcabus

    pubsub = r.pubsub()
    pubsub.subscribe("BUS_BOARDING_AREA_CHANGE")

    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            school = db.schools.find_one({"_id": ObjectId(data["schoolID"])})
            if school and school["public_scopes"] and "read" in school["public_scopes"]:
                print("Sending notification")

if __name__ == "__main__":
    main()