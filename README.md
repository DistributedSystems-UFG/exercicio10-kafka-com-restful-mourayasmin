# Exercício 10 - Kafka com REST API

## Arquitetura

```
Produtor → Processador → Consumer Service → Database
(Sensor)   (Média 2h)    (Kafka→SQLite)      (SQLite)
                                                 ↓
Cliente ←────────── REST API ←──────────────────┘
        (HTTP/JSON)
```

**Fluxo de dados**:
1. **Produtor** simula sensor e publica leituras no Kafka (`raw-temperature-events`)
2. **Processador** consome, calcula média móvel e republica (`processed-temperature-events`)
3. **Consumer Service** persiste estatísticas no SQLite
4. **REST Server** expõe API HTTP/JSON de consulta (FastAPI)
5. **Cliente** consulta dados via HTTP

## Execução

### 1. Instalar dependências
```bash
pip3 install -r requirements.txt
```

### 2. Subir Kafka
```bash
docker-compose up -d
```

### 3. Executar componentes (terminais separados; nessa ordem)
```bash
# Terminal 1: Produtor
python3 producer/sensor_producer.py

# Terminal 2: Processador
python3 processor/event_processor.py

# Terminal 3: Consumer Service
python3 service/consumer_service.py

# Terminal 4: REST Server
python3 service/rest_server.py

# Terminal 5: Cliente (aguarde 2-3 min antes)
python3 client/rest_client.py
```

## API REST

### Endpoints

- **GET** `/sensors/{sensor_id}/latest` - Última leitura
- **GET** `/sensors/{sensor_id}/stats?hours=N` - Estatísticas das últimas N horas
- **GET** `/sensors/{sensor_id}/historical?start=X&end=Y` - Dados históricos
- **GET** `/health` - Health check

### Documentação Interativa

FastAPI gera automaticamente documentação interativa:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Exemplos com curl

```bash
# Última leitura
curl http://localhost:8000/sensors/sensor-001/latest

# Estatísticas das últimas 6 horas
curl "http://localhost:8000/sensors/sensor-001/stats?hours=6"

# Dados históricos
curl "http://localhost:8000/sensors/sensor-001/historical?start=2026-05-21T10:00:00&end=2026-05-21T12:00:00"

# Health check
curl http://localhost:8000/health
```
