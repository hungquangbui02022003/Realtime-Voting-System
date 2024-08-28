import random
import time
from datetime import datetime, timezone

import psycopg2
import simplejson as json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Kafka consumer configuration
consumer = KafkaConsumer(
    'voters_topic',
    bootstrap_servers='localhost:9092',
    group_id='voting-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


if __name__ == "__main__":
    conn = psycopg2.connect(host='localhost', port ='5432', database='voting', user='postgres', password='1234')
    cur = conn.cursor()

    # candidates
    cur.execute("""
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) t;
    """)
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            if not msg_pack:
                continue

            for tp, messages in msg_pack.items():
                for msg in messages:
                    voter = msg.value
                    chosen_candidate = random.choice(candidates)
                    vote = voter | chosen_candidate | {
                        'voting_time' : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "vote": 1
                    }

                    try:
                        print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']}")
                        cur.execute("""
                                INSERT INTO votes (voter_id, candidate_id, voting_time)
                                VALUES (%s, %s, %s)
                            """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                        conn.commit()

                        producer.send(
                            'votes_topic',
                            key=vote["voter_id"],
                            value=vote
                        )
                    except Exception as e:
                        print(f"Error: {e}")
                        continue
            time.sleep(0.2)
    except KafkaError as e:
        print(f"Kafka error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        producer.close()
