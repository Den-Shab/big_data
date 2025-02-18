## Для запуска, необоходимо выполнить следующие действия:
-  Склонировать репозиторий
-  Установить все необходимые зависимости при помощи файла requrements.txt
-  Скачать данные по ссылке https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx и разместить файл в директории репозитория
-  docker-compose up -d
-  python generate_data.py
-  python producer.py
-  streamlit run dashboard.py
В dashboard и producer можно выбрать n для работы с определенным количеством данных
## Примеры графиков из dashboard:

![newplot(3)](https://github.com/user-attachments/assets/5fe37291-f16f-4006-8194-f0df50efbe61)
![newplot(5)](https://github.com/user-attachments/assets/baccbfec-b4ec-42c0-8869-27b9fcec5e70)
![newplot(2)](https://github.com/user-attachments/assets/5f4160d9-de36-4413-9d76-e71c3ac2ae4e)
