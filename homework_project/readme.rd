FoodDeliveryBI - Главный координатор, запускает весь пайплайн (нужно запустить его и кафку)
FoodOrdersToKafka - Отправка данных в кафку (запустить после запуска FoodDeliveryBI)

KafkaToRawProcessor - Читает данные из Kafka → сохраняет в Raw-слой (Parquet)
RawToMartProcessor - Преобразует Raw-данные → витрины Mart (ORC)
BIConnector - Настраивает доступ к данным для бизнес аналитики, сохранение в таблицы и csv (файлы в дир output/export/..)