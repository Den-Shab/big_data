import streamlit as st
import pandas as pd
import plotly.express as px
from confluent_kafka import Consumer
import json

st.title("Real-Time Retail Analytics")

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'retail-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['retail'])

n = 5000
data = []
while len(data) < n:
    msg = consumer.poll(0.1)
    if msg is None:
        continue
    record = json.loads(msg.value())
    data.append({
        'time': record['timestamp'],
        'product': record['product_id'],
        'price': record['price'],
        'quantity': record['quantity'],
        'description': record['description']
    })

df = pd.DataFrame(data)

if not df.empty:
    top_products = df.groupby('description')['quantity'].sum().nlargest(10)
    st.plotly_chart(px.bar(top_products, title="Top Selling Products"))
    
    df['time'] = pd.to_datetime(df['time'])
    hourly_sales = df.resample('H', on='time')['quantity'].sum()
    st.plotly_chart(px.line(hourly_sales, title="Hourly Sales Volume"))
    
    st.plotly_chart(px.histogram(df, x='price', nbins=24, title="Price Distribution"))

consumer.close()
