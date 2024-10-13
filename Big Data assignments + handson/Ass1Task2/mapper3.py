#!/usr/bin/env python3
import sys

def get_endpoint_cost(apiPath, status):
    cost_map = {
        'user/profile': 100, 
        'user/settings': 200,
        'order/history': 300, 
        'order/checkout': 400,
        'product/details': 500,
        'product/search': 600,
        'cart/add': 700,
        'cart/remove': 800,
        'payment/submit': 900,
        'support/ticket': 1000
    }
    return cost_map.get(apiPath, 0) if status == "200" else 0

for record in sys.stdin:
    data = record.strip().split()
    
    if len(data) == 7:
        idRequest, idClient, apiPath, time, serversDown, predictedCode, actualCode = data
        
        cost = get_endpoint_cost(apiPath, actualCode)
        
        print(f"{idClient} {cost} {predictedCode} {actualCode}")

