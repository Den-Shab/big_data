import pandas as pd
from datetime import datetime

df = pd.read_excel('online_retail_II.xlsx')

df = df.rename(columns={
    'InvoiceDate': 'timestamp',
    'Quantity': 'quantity',
    'Price': 'price',
    'Customer ID': 'customer_id',
    'StockCode': 'product_id',
    'Description': 'description'
})

df = df[~df['Invoice'].astype(str).str.startswith('C')]
df = df.dropna()
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

df[['timestamp', 'product_id', 'quantity', 'price', 'customer_id', 'description']].to_csv('retail_data.csv', index=False)