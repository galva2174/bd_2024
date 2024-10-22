#!/usr/bin/env python3
import sys
import json
from kafka import KafkaConsumer

def main():
    topic = sys.argv[1]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        
    )

    language_count = {}
    category_count = {}
    category_passed = {}

    for message in consumer:
        event = message.value
        
        if event.get("event_type") == "EOF":
            break
        
       
        category = event.get("category")
        status = event.get("status")
        language = event.get("language")

        if language:
            if language not in language_count:
                language_count[language] = 0
            language_count[language] += 1
      
        if category:
            if category not in category_count:
                category_count[category] = 0
            category_count[category] += 1
       
        if status == "Passed" and category:
            if category not in category_passed:
                category_passed[category] = 0
            category_passed[category] += 1

    most_difficult = []
    min_pass_ratio = float('inf')
    
    for category in sorted(category_count):
        total_submissions = category_count[category]
        passed_submissions = category_passed.get(category, 0)

        if total_submissions > 0:
            pass_ratio = passed_submissions / total_submissions
        else:
            pass_ratio = float('inf')
      
        if pass_ratio < min_pass_ratio:
            min_pass_ratio = pass_ratio
            most_difficult = [category]
        elif pass_ratio == min_pass_ratio:
            most_difficult.append(category)

    
    max_language_count = max(language_count.values(), default=0)
    most_used_language = sorted([lang for lang, count in language_count.items() if count == max_language_count])

    result = {
        "most_used_language": most_used_language,
        "most_difficult_category": most_difficult
    }

    print(json.dumps(result, indent=4))

if __name__ == "__main__":
    main()

