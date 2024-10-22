#!/usr/bin/env python3
import sys
from kafka import KafkaProducer
import json

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092', 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for line in sys.stdin:
        if line.strip() == "EOF":
            break

        parts = line.strip().split()
        if not parts:
            continue

        event_type = parts[0]
        data = {}

        if event_type == "problem":
            data = {
                "event_type": "problem",
                "user_id": parts[1],
                "problem_id": parts[2],
                "category": parts[3],
                "difficulty": parts[4],
                "submission_id": parts[5],
                "status": parts[6],
                "language": parts[7],
                "runtime": int(parts[8])
            }
            producer.send(sys.argv[1], value=data)
            
            

        elif event_type == "competition":
            data = {
                "event_type": "competition",
                "comp_id": parts[1],
                "user_id": parts[2],
                "problem_id": parts[3],
                "category": parts[4],
                "difficulty": parts[5],
                "comp_submission_id": parts[6],
                "status": parts[7],
                "language": parts[8],
                "runtime": int(parts[9]),
                "time_taken": int(parts[10])
            }
            producer.send(sys.argv[2], value=data)
            

        elif event_type == "solution":
            data = {
                "event_type": "solution",
                "user_id": parts[1],
                "problem_id": parts[2],
                "submission_id": parts[3],
                "upvotes": int(parts[4])
            }
            producer.send(sys.argv[3], value=data)
            

    producer.send(sys.argv[1], value={"event_type": "EOF"})
    producer.send(sys.argv[2], value={"event_type": "EOF"})
    producer.send(sys.argv[3], value={"event_type": "EOF"})
    

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()

