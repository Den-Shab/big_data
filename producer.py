import pandas as pd
import json
import joblib
import numpy as np
from confluent_kafka import Consumer, Producer
from sklearn.linear_model import SGDRegressor
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline

class IncrementalLearner:
    def __init__(self):
        self.model = make_pipeline(
            StandardScaler(),
            SGDRegressor(random_state=42))
        self.vectorizer = HashingVectorizer(n_features=100, alternate_sign=False)
        self.batch = []
        self.batch_size = 100
        self.feature_names = ['price', 'hour', 'day_of_week']
        
    def process_message(self, message):
        data = json.loads(message.value())
        self.batch.append(data)
        
        if len(self.batch) >= self.batch_size:
            self._update_model()
            self.batch = []

    def _preprocess(self, batch):
        df = pd.DataFrame(batch)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df = df.drop(['description'], axis=1)
        
        # Обработка категориальных признаков
        product_features = self.vectorizer.transform(df['product_id'])
        num_features = df[self.feature_names]
        
        X = np.hstack([num_features, product_features.toarray()])
        y = df['quantity'].values
        
        return X, y

    def _update_model(self):
        X, y = self._preprocess(self.batch)
        
        if not hasattr(self.model, 'coef_'):
            self.model.fit(X, y)
        else:
            self.model.partial_fit(X, y)
            
        joblib.dump(self.model, 'online_model.pkl')

def train_from_kafka(n):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'online-learner',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['retail'])

    learner = IncrementalLearner()
    msg_c = 0
    while True:
        msg = consumer.poll(0.1)
        if msg is not None:
            msg_c += 1
        if msg_c == n:
            consumer.close()
            break
        if msg and not msg.error():
            print(msg.value())
            learner.process_message(msg)

def produce_data():
    df = pd.read_csv('retail_data.csv')
    producer = Producer({'bootstrap.servers': 'localhost:9092',
                        'queue.buffering.max.messages': 1000000}) 
    
    for _, row in df.iterrows():
        producer.produce(
            'retail',
            json.dumps({
                'timestamp': str(row['timestamp']),
                'product_id': row['product_id'],
                'quantity': row['quantity'],
                'price': row['price'],
                'description': row['description']
            })
        )
    producer.flush()

if __name__ == "__main__":
    n = 1000
    produce_data()
    train_from_kafka(n)