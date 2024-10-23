## Task 1: Store Profit and Loss
## Story Background:
You are given data from a retail chain in India that sells a wide range of products, including groceries, home appliances, beauty items, apparel and many more goods in multiple cities across the country. However, not all stores in the chain sell every product category. Each store has its own top-selling categories based on consumer demand. Only these top-selling categories will determine the profit gained or the loss incurred by a store.

Given the input, your task is to determine:

The number of stores that are profitable for each city.
The number of stores operating at a loss for each city.
## Points to be Considered:
Sales data (Revenue and COGS) for a product category (top-selling or not) may or may not be recorded.
Net results (Profit or Loss) for each store is only calculated if there exists Sales data (Revenue and COGS) for atleast some top-selling category.
If Net results > 0, it is a profitable store, otherwise it's incurring a loss.
Mapper
Input : Array of JSON Objects

Here's a prettified version of an example JSON object:
{
  "city": "Bangalore",
  "store_id": "ST01293",
  "categories": [
    "Electronics",
    "Groceries"
  ],
  "sales_data": {
    "Electronics": {
      "revenue": 600000,
      "cogs": 500000
    },
    "Groceries": {
      "revenue": 250000,
      "cogs": 270000
    }
  }
}  
Example Input :

[
{"city": "Bangalore", "store_id": "ST01293", "categories": ["Electronics", "Groceries"], "sales_data": {"Electronics": {"revenue": 600000, "cogs": 500000}, "Groceries": {"revenue": 250000, "cogs": 270000}}},
{"city": "Chennai", "store_id": "ST04567", "categories": ["Pharmacy and Health", "Kitchen", "Toys and Stationery"], "sales_data": {"Kitchen": {"revenue": 800000, "cogs": 900000}, "Toys and Stationery": {"revenue": 300000, "cogs": 450000}, "Pharmacy and Health": {"revenue": 450000, "cogs": 470000}}},
{"city": "Mumbai", "store_id": "ST05432", "categories": ["Books and Magazines", "Pharmacy and Health"], "sales_data": {"Books and Magazines": {"revenue": 200000, "cogs": 150000}, "Pharmacy and Health": {"revenue": 350000, "cogs": 300000}}},
{"city": "Mumbai", "store_id": "ST08345", "categories": ["Groceries"], "sales_data": {"Groceries": {"revenue": 700000, "cogs": 650000}}},
{"city": "Chennai", "store_id": "ST06789", "categories": ["Home Decor", "Apparel"], "sales_data": {"Apparel": {"revenue": 850000, "cogs": 800000}, "Home Decor": {"revenue": 500000, "cogs": 450000}}},
{"city": "Bangalore", "store_id": "ST09874", "categories": ["Apparel"], "sales_data": {"Apparel": {"revenue": 620000, "cogs": 600000}}}
]
  
Net Returns Formula = Revenue - COGS
COGS = Cost of Goods and Services  

Reducer  
Input : Same format as Mapper output  

Output : Independent JSON Objects  

output of the reducer has the following keys : name of the city, number of profitable stores and number of stores at a loss.  
Example Output:  

{"city": "Bangalore", "profit_stores": 2, "loss_stores": 0}
{"city": "Chennai", "profit_stores": 1, "loss_stores": 1}
{"city": "Mumbai", "profit_stores": 2, "loss_stores": 0}

## Instructions
Write a python mapper
Name : mapper.py
Read the specification for input and output as mentioned above.
Only packages that can be imported are : json and sys  
Name : reducer.py
Write a python reducer to perform the aggregation

