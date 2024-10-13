#!/usr/bin/env python3
import sys
import json

def main():
    hashmap = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        city, ps, ls = line.split('\t')
        
        if city not in hashmap:
            hashmap[city] = {'ps': 0, 'ls': 0}

        hashmap[city]['ps'] += int(ps)
        hashmap[city]['ls'] += int(ls)

    sorted_cities = sorted(hashmap.keys())

    for city in sorted_cities:
        counts = hashmap[city]
        output = {
            "city": city,
            "profit_stores": counts['ps'],
            "loss_stores": counts['ls']
        }
        print(json.dumps(output))
        sys.stdout.flush()

if __name__ == "__main__":
    main()

