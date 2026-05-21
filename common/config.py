# Configurações compartilhadas

# Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_RAW_EVENTS = 'raw-temperature-events'
TOPIC_PROCESSED_EVENTS = 'processed-temperature-events'

# REST API
REST_SERVER_HOST = 'localhost'
REST_SERVER_PORT = 8000

# Database
DATABASE_PATH = 'temperature.db'

# Sensor simulation
SENSOR_ID = 'sensor-001'
TEMP_MIN = 20.0
TEMP_MAX = 35.0
READING_INTERVAL_SECONDS = 5
