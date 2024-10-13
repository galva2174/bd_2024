#!/usr/bin/env python3
import sys
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split()
    if len(parts) == 2:
        reqid = parts[0]
        statuscode = parts[1]
        print(f"{reqid} {statuscode}")
    elif len(parts) > 3:
        reqid = parts[0]
        clientid = parts[1]
        endpoint = parts[2]
        timestamp = parts[3]
        serversdown = parts[4] if len(parts) > 4 else '0.0'
        print(f"{reqid} {clientid} {endpoint} {timestamp} {serversdown}")

