#!/usr/bin/env python3
import sys

customer_bills = {}

for line in sys.stdin:
    customer_id, discounted_price = line.strip().split("\t")
    discounted_price = float(discounted_price)
    
    if customer_id in customer_bills:
        customer_bills[customer_id] += discounted_price
    else:
        customer_bills[customer_id] = discounted_price

for customer_id, total_bill in customer_bills.items():
    print(f"{customer_id} {total_bill:.1f}\t")
