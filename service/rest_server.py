# Servidor REST - TemperatureService
# Endpoints:
# - GET /sensors/{sensor_id}/latest - última leitura do sensor
# - GET /sensors/{sensor_id}/stats?hours=N - estatísticas de média
# - GET /sensors/{sensor_id}/historical?start=X&end=Y - dados históricos

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

from service.storage import TemperatureStorage
from common.config import REST_SERVER_HOST, REST_SERVER_PORT


# Modelos de resposta (Pydantic para validação e documentação automática)
class Reading(BaseModel):
    """
    Representa uma leitura de temperatura.

    CONCEITO: Pydantic valida tipos automaticamente e gera
    documentação OpenAPI (acessível em /docs).
    """
    sensor_id: str
    temperature: float
    timestamp: str


class Statistics(BaseModel):
    """
    Representa estatísticas de temperatura (média móvel).
    """
    sensor_id: str
    avg_temperature: float
    sample_count: int
    window_start: str
    window_end: str


class HistoricalResponse(BaseModel):
    """
    Resposta com múltiplas estatísticas históricas.
    """
    data: List[Statistics]


# Criar aplicação FastAPI
app = FastAPI(
    title="Temperature Service API",
    description="API REST para consulta de dados de temperatura de sensores IoT",
    version="1.0.0"
)

# Storage (banco de dados)
storage = TemperatureStorage()


@app.get("/")
def root():
    """
    Endpoint raiz - informações da API.

    CONCEITO: RESTful APIs geralmente expõem um endpoint root
    com metadados sobre o serviço.
    """
    return {
        "service": "Temperature Service API",
        "version": "1.0.0",
        "endpoints": {
            "latest": "/sensors/{sensor_id}/latest",
            "stats": "/sensors/{sensor_id}/stats?hours=N",
            "historical": "/sensors/{sensor_id}/historical?start=X&end=Y"
        },
        "docs": "/docs"
    }


@app.get("/sensors/{sensor_id}/latest", response_model=Reading)
def get_latest_reading(sensor_id: str):
    """
    Retorna a última leitura do sensor.

    CONCEITO REST:
    - Método: GET (leitura, idempotente)
    - Recurso: /sensors/{sensor_id}/latest
    - Status: 200 OK ou 404 Not Found

    PARADIGMA: Resource-Oriented Architecture
    - URL representa um recurso (última leitura)
    - Verbo HTTP indica a operação (GET = consultar)

    Diferença vs gRPC:
    - REST: URL + verbo HTTP (padrão web)
    - gRPC: método remoto específico (GetLatestReading)
    """
    print(f"GET /sensors/{sensor_id}/latest")

    # Tentar buscar leitura bruta
    reading = storage.get_latest_reading(sensor_id)

    if reading:
        return Reading(**reading)

    # Se não houver leitura bruta, buscar última estatística processada
    stats_list = storage.get_avg_stats(sensor_id, hours=24)

    if stats_list:
        latest_stat = stats_list[0]
        return Reading(
            sensor_id=latest_stat['sensor_id'],
            temperature=latest_stat['avg_temperature'],
            timestamp=latest_stat['window_end']
        )

    # Nenhum dado encontrado
    raise HTTPException(status_code=404, detail=f"Sensor {sensor_id} not found")


@app.get("/sensors/{sensor_id}/stats", response_model=Statistics)
def get_average_stats(
    sensor_id: str,
    hours: int = Query(default=2, ge=1, le=168, description="Número de horas (1-168)")
):
    """
    Retorna estatísticas das últimas N horas.

    CONCEITO REST:
    - Query parameters (?hours=N) para filtros
    - FastAPI valida automaticamente (1 <= hours <= 168)

    Diferença vs gRPC:
    - REST: query string natural para navegadores
    - gRPC: campos em mensagem Protobuf (mais estruturado)
    """
    print(f"GET /sensors/{sensor_id}/stats?hours={hours}")

    stats_list = storage.get_avg_stats(sensor_id, hours)

    if stats_list:
        latest = stats_list[0]
        return Statistics(**latest)

    raise HTTPException(
        status_code=404,
        detail=f"No data for sensor {sensor_id} in last {hours} hours"
    )


@app.get("/sensors/{sensor_id}/historical", response_model=HistoricalResponse)
def get_historical_data(
    sensor_id: str,
    start: str = Query(..., description="Timestamp inicial (ISO 8601)"),
    end: str = Query(..., description="Timestamp final (ISO 8601)")
):
    """
    Retorna dados históricos em um período.

    CONCEITO REST:
    - Query parameters obrigatórios (...) para período
    - Formato ISO 8601 para timestamps (padrão web)

    Exemplo:
    /sensors/sensor-001/historical?start=2026-05-21T10:00:00&end=2026-05-21T12:00:00
    """
    print(f"GET /sensors/{sensor_id}/historical?start={start}&end={end}")

    stats_list = storage.get_historical_data(sensor_id, start, end)

    # Converter lista de dicts para lista de objetos Statistics
    data = [Statistics(**stats) for stats in stats_list]

    return HistoricalResponse(data=data)


@app.get("/health")
def health_check():
    """
    Endpoint de health check.

    CONCEITO: Comum em APIs REST para monitoramento.
    Load balancers e orchestradores (Kubernetes) usam isso.
    """
    return {"status": "healthy", "database": "connected"}


def main():
    """
    Inicia o servidor REST com Uvicorn.

    CONCEITO: FastAPI usa ASGI (Asynchronous Server Gateway Interface).
    Uvicorn é um servidor ASGI de alta performance.

    Diferença vs gRPC:
    - REST/HTTP: ubíquo, navegadores nativos, ferramentas abundantes
    - gRPC: requer biblioteca cliente, melhor para microsserviços
    """
    print("=" * 70)
    print("SERVIDOR REST - TEMPERATURE SERVICE")
    print("=" * 70)
    print(f"Host: {REST_SERVER_HOST}")
    print(f"Port: {REST_SERVER_PORT}")
    print(f"Documentação interativa: http://{REST_SERVER_HOST}:{REST_SERVER_PORT}/docs")
    print("=" * 70)
    print("\nEndpoints disponíveis:")
    print(f"   GET  /sensors/{{sensor_id}}/latest")
    print(f"   GET  /sensors/{{sensor_id}}/stats?hours=N")
    print(f"   GET  /sensors/{{sensor_id}}/historical?start=X&end=Y")
    print(f"   GET  /health")
    print("=" * 70)
    print("\nCONCEITO: REST usa recursos (URLs) + verbos HTTP.")
    print("   FastAPI gera documentação automática em /docs\n")
    print("-" * 70)
    print("Servidor rodando... (Ctrl+C para parar)\n")

    uvicorn.run(
        app,
        host=REST_SERVER_HOST,
        port=REST_SERVER_PORT,
        log_level="info"
    )


if __name__ == "__main__":
    main()
