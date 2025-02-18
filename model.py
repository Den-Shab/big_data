# from sklearn.ensemble import RandomForestRegressor
# from sklearn.model_selection import train_test_split
# import pandas as pd
# import joblib

# df = pd.read_csv('sales_data.csv')
# X = pd.get_dummies(df[['product_id', 'price']])
# y = df['quantity']

# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# model = RandomForestRegressor()
# model.fit(X_train, y_train)
# joblib.dump(model, 'demand_model.pkl')
import time
import json
import joblib
import pandas as pd
from confluent_kafka import Producer, Consumer
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder

class StreamingModel:
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=100)
        self.le = LabelEncoder()
        self.batch = []
        self.batch_size = 100  # Размер батча для переобучения
        self.is_fitted = False

    def preprocess(self, data):
        # Преобразование временной метки
        dt = pd.to_datetime(data['timestamp'])
        data['hour'] = dt.hour
        data['day_of_week'] = dt.dayofweek
        
        # Кодирование product_id
        data['product_id'] = self.le.transform([data['product_id']])[0]
        
        # Формирование признаков
        features = {
            'product_id': data['product_id'],
            'price': data['price'],
            'hour': data['hour'],
            'day_of_week': data['day_of_week']
        }
        return features, data['quantity']

    def update_model(self, X, y):
        if not self.is_fitted:
            self.model.fit(X, y)
            self.is_fitted = True
        else:
            # Для потокового обновления можно использовать warm_start
            self.model.n_estimators += 10
            self.model.fit(X, y)

def model_training():
    # Конфигурация Kafka
    consumer_conf = {
        'bootstrap.servers': 'localhost:9093',
        'group.id': 'retail-model',
        'auto.offset.reset': 'earliest'
    }
    
    producer_conf = {'bootstrap.servers': 'localhost:9093'}

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    
    consumer.subscribe(['retail-raw'])
    model = StreamingModel()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                features, target = model.preprocess(data)
                
                # Накопление батча
                model.batch.append((features, target))
                
                # Переобучение при накоплении достаточного количества данных
                if len(model.batch) >= model.batch_size:
                    X = pd.DataFrame([x[0] for x in model.batch])
                    y = [x[1] for x in model.batch]
                    
                    model.update_model(X, y)
                    
                    # Сброс батча
                    model.batch = []
                    
                    # Сохранение модели
                    joblib.dump((model.model, model.le), 'streaming_model.pkl')
                    
                    # Отправка метрик
                    producer.produce(
                        'model-metrics',
                        json.dumps({'status': 'model_updated', 'n_samples': X.shape[0]})
                    )
                
                # Прогнозирование
                prediction = model.model.predict(pd.DataFrame([features]))[0]
                
                # Отправка прогноза
                producer.produce(
                    'retail-predictions',
                    json.dumps({
                        'actual': target,
                        'predicted': float(prediction),
                        'product_id': data['product_id']
                    })
                
            except json.JSONDecodeError:
                print("Invalid JSON message")
            except Exception as e:
                print(f"Processing error: {str(e)}")
                
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    model_training()
