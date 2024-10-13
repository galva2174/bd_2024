#!/usr/bin/env python3
import sys

previous_request_id = None
predicted_status = None

for line in sys.stdin:
    parts = line.strip().split()

    if len(parts) == 2:
        request_id, predicted_status = parts
        previous_request_id = request_id  
        
    elif len(parts) > 2:
        request_id = parts[0]
        
        if request_id == previous_request_id and predicted_status is not None:
            print(f"{line.strip()} {predicted_status}")
            predicted_status = None  
        else:
            print(line.strip())

