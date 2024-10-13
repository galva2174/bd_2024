#!/usr/bin/env python3
import sys

for line in sys.stdin:
    customer_id, customer_points, product_id, quantity, total_price = line.strip().split("\t")
    
    customer_points = int(customer_points)
    total_price = float(total_price)
    
    if customer_points < 100:
        discount = 0
    elif 100 <= customer_points < 200:
        discount = 0.1
    elif 200 <= customer_points < 300:
        discount = 0.2
    elif 300 <= customer_points < 400:
        discount = 0.3
    else:
        discount = 0.4
    
    discounted_price = total_price * (1 - discount)
    
    print(f"{customer_id}\t{discounted_price}")
