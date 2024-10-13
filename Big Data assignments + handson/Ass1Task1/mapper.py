#!/usr/bin/env python3
import json
import sys

def profitloss(sd, categories):
    pl = 0
    found = False 
    for category in categories:
        if category in sd:
            cd = sd[category]
            if 'revenue' in cd and 'cogs' in cd:
                revenue = cd['revenue']
                cogs = cd['cogs']
                pl += (revenue - cogs)
                found = True
    return pl if found else None

def process_record(record):
    city = record.get('city', 'Unknown')
    categories = record.get('categories', [])
    sd = record.get('sales_data', {})

    if not categories or not sd:
        return None

    answer = profitloss(sd, categories)
    
    if answer is None:
        return None

    if answer > 0:
        return (city, '1', '0')
    else:
        return (city, '0', '1')

def main():
    buffer = ""

    for line in sys.stdin:
        line = line.strip()
        
        if line == "[" or line == "]":
            continue

        if line.endswith(","):
            line = line[:-1]

        buffer += line
        record = json.loads(buffer)
        buffer = ""

        result = process_record(record)
        if result is not None:
                print(f"{result[0]}\t{result[1]}\t{result[2]}")
                sys.stdout.flush()
if __name__ == "__main__":
    main()

