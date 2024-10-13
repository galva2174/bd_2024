#!/usr/bin/env python3
import sys

for line in sys.stdin:
    data = line.strip().split()
    
    if len(data) == 4:  # Customer Purchases
        customer_id, customer_points, product_id, quantity = data
        print(f"{product_id}\t{customer_id}\t{customer_points}\t{quantity}")
    
    elif len(data) == 2:  # Product Prices
        product_id, price = data
        print(f"{product_id}\t{price}")
