import os
import redis
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
import firebase_admin
from firebase_admin import credentials, messaging
import datetime

firebase_admin.initialize_app(credentials.Certificate("serviceaccountkey.json"))

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
                now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
                school_id = data["schoolID"]
                bus_id = data["busID"]
                bus_name = data["busName"]
                boarding_area = data["newBoardingArea"]
                message = messaging.Message(
                    data={
                        "bus": bus_id,
                        "boarding_area": boarding_area,
                        "location": boarding_area,
                        "invalidate_time": data["invalidateTime"],
                        "time": now
                    },
                    notification=messaging.Notification(
                        title=f"{bus_name} boarding at {boarding_area}",
                        body=f"The bus for \"{bus_name}\" is now at {boarding_area}."
                    ),
                    apns=messaging.APNSConfig(payload=messaging.APNSPayload(messaging.Aps(sound="default"))),
                    topic=f"school.{school_id}.bus.{bus_id}"
                )
                messaging.send(message)

if __name__ == "__main__":
    main()
