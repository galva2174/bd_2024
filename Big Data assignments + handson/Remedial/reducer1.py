#!/usr/bin/env python3
import sys

product_prices = {}

for line in sys.stdin:
    data = line.strip().split("\t")
    
    if len(data) == 4:
        product_id, customer_id, customer_points, quantity = data
        quantity = int(quantity)
        if product_id in product_prices:
            price = product_prices[product_id]
            total_price = price * quantity
            print(f"{customer_id}\t{customer_points}\t{product_id}\t{quantity}\t{total_price}")
    
    elif len(data) == 2:  # Product Prices
        product_id, price = data
        product_prices[product_id] = float(price)
