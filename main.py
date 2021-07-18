import os
import redis
import json

def main():
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )

    pubsub = r.pubsub()
    pubsub.subscribe("BUS_BOARDING_AREA_CHANGE")

    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            print(data)

if __name__ == "__main__":
    main()