#!/usr/bin/env python3
import sys
import json
import math
from kafka import KafkaConsumer

INITIAL_ELO = 1200.0
K = 32.0
STATUS_SCORE = {"Passed": 1.0, "TLE": 0.2, "Failed": -0.3}
DIFFICULTY_SCORE = {"Hard": 1.0, "Medium": 0.7, "Easy": 0.3}

def calculate_submission_points(status, difficulty, runtime):
    status_score = STATUS_SCORE.get(status, 0)
    difficulty_score = DIFFICULTY_SCORE.get(difficulty, 0)
    runtime_bonus = float(10000.0 / int(runtime)) if runtime > 0 else 0.0
    submission_points = K * (status_score * difficulty_score) + runtime_bonus
    return float(submission_points)

def main():
    consumer = KafkaConsumer(
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )
    
    user_upvotes = {}
    user_elo = {}
    processed_submissions = set()
    eof_status = {
        sys.argv[1]: False,
        sys.argv[2]: False,
        sys.argv[3]: False
    }

    def init_user_if_not_exists(user_id):
        if user_id not in user_elo:
            user_elo[user_id] = float(INITIAL_ELO)

    for message in consumer:
        data = message.value
        
        if data["event_type"] == "problem":
            submission_id = data["submission_id"]
            if submission_id in processed_submissions:
                continue
            processed_submissions.add(submission_id)
            user_id = data["user_id"]
            status = data["status"]
            difficulty = data["difficulty"]
            runtime = data["runtime"]
            init_user_if_not_exists(user_id)
            submission_points = calculate_submission_points(status, difficulty, runtime)
            user_elo[user_id] += float(submission_points)
        
        elif data["event_type"] == "competition":
            comp_submission_id = data["comp_submission_id"]
            if comp_submission_id in processed_submissions:
                continue
            processed_submissions.add(comp_submission_id)
            user_id = data["user_id"]
            status = data["status"]
            difficulty = data["difficulty"]
            runtime = data["runtime"]
            init_user_if_not_exists(user_id)
            submission_points = calculate_submission_points(status, difficulty, runtime)
            user_elo[user_id] += float(submission_points)
        
        elif data["event_type"] == "solution":
            user_id = data["user_id"]
            upvotes = data["upvotes"]
            if user_id not in user_upvotes:
                user_upvotes[user_id] = 0
            user_upvotes[user_id] += upvotes

        elif data["event_type"] == "EOF":
            eof_status[message.topic] = True

        if all(eof_status.values()):
            break

    max_upvotes = max(user_upvotes.values(), default=0)
    best_contributors = sorted([user for user, upvotes in user_upvotes.items() if upvotes == max_upvotes])

    sorted_elo_ratings = {user_id: math.floor(float(elo)) for user_id, elo in sorted(user_elo.items())}

    output = {
        "best_contributor": best_contributors,
        "user_elo_rating": sorted_elo_ratings
    }

    print(json.dumps(output, indent=4))

if __name__ == "__main__":
    main()

