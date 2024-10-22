#!/usr/bin/env python3
import sys
import json
from kafka import KafkaConsumer

def calculate_points(status, difficulty, runtime, time_taken):
    if status == "Passed":
        status_score = 100
    elif status == "TLE":
        status_score = 20
    else:
        status_score = 0

    difficulty_score = {
        "Hard": 3,
        "Medium": 2,
        "Easy": 1
    }.get(difficulty, 0)

    runtime_bonus = 10000 / runtime if runtime > 0 else 0
    time_taken_penalty = 0.25 * time_taken
    bonus = max(1, (1 + runtime_bonus - time_taken_penalty))

    return int(status_score * difficulty_score * bonus)

def main():
    competition_scores = {}
    topic = sys.argv[2]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    for message in consumer:
        event = message.value
        
        if event.get("event_type") == "EOF":
            break

        comp_id = event["comp_id"]
        user_id = event["user_id"]
        status = event["status"]
        difficulty = event["difficulty"]
        runtime = event["runtime"]
        time_taken = event["time_taken"]

        points = calculate_points(status, difficulty, runtime, time_taken)

        if comp_id not in competition_scores:
            competition_scores[comp_id] = {}

        if user_id not in competition_scores[comp_id]:
            competition_scores[comp_id][user_id] = 0
        
        competition_scores[comp_id][user_id] += points

    sorted_leaderboard = {}
    for comp_id in sorted(competition_scores.keys()):
        sorted_leaderboard[comp_id] = dict(sorted(competition_scores[comp_id].items()))

    print(json.dumps(sorted_leaderboard, indent=4))

if __name__ == "__main__":
    main()

