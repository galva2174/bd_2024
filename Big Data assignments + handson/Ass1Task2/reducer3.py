#!/usr/bin/env python3
import sys

def print_client_summary(client, correct_preds, req_count, price_sum):
    print(f"{client} {correct_preds}/{req_count} {price_sum}")

currentClient = None
requestCount = 0
successfulPredictions = 0
priceSum = 0

for entry in sys.stdin:
    client, costStr, predStatus, actualStatus = entry.strip().split()

    if client != currentClient:
        if currentClient is not None:
            print_client_summary(currentClient, successfulPredictions, requestCount, priceSum)
        currentClient = client
        requestCount = 0
        successfulPredictions = 0
        priceSum = 0

    requestCount += 1
    priceSum += int(costStr)

    if predStatus == actualStatus:
        successfulPredictions += 1

if currentClient is not None:
    print_client_summary(currentClient, successfulPredictions, requestCount, priceSum)

