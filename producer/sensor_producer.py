# Produtor - Sensor simulado de temperatura
# Publica eventos no tópico Kafka: raw-temperature-events

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW_EVENTS,
    SENSOR_ID,
    TEMP_MIN,
    TEMP_MAX,
    READING_INTERVAL_SECONDS
)


def main():
    """
    CONCEITO: KAFKA PRODUCER

    Um Producer publica mensagens em um "tópico" Kafka.
    Tópicos são como canais de mensagens - quem se inscrever
    nele (consumidores) vai receber as mensagens.

    PARADIGMA: Pub-Sub (Publicação-Subscrição)
    - Produtor NÃO conhece os consumidores
    - Desacoplamento: se adicionar novo consumidor, produtor não muda
    - Assíncrono: produtor não espera resposta
    """

    # 1. Criar produtor conectado ao Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # Onde está o Kafka (localhost:9092)
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Converte dict → JSON → bytes
    )

    print("=" * 60)
    print("SENSOR DE TEMPERATURA - PRODUTOR KAFKA")
    print("=" * 60)
    print(f"Conectado ao Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Publicando no tópico: {TOPIC_RAW_EVENTS}")
    print(f"Sensor ID: {SENSOR_ID}")
    print(f"Faixa: {TEMP_MIN}°C - {TEMP_MAX}°C")
    print(f"Intervalo: {READING_INTERVAL_SECONDS}s")
    print("=" * 60)
    print("\nCONCEITO: Cada evento representa uma leitura do sensor.")
    print("   Esses eventos vão para o Kafka e ficam lá até serem consumidos.\n")
    print("-" * 60)

    # 2. Simular temperatura inicial aleatória
    current_temp = random.uniform(TEMP_MIN, TEMP_MAX)

    # 3. Loop infinito gerando leituras
    try:
        iteration = 0
        while True:
            iteration += 1

            # Simular variação realista: ±0.5°C por leitura
            # (temperatura não muda bruscamente, só gradualmente)
            variation = random.uniform(-0.5, 0.5)
            current_temp += variation

            # Manter dentro dos limites
            current_temp = max(TEMP_MIN, min(TEMP_MAX, current_temp))

            # Criar evento (mensagem)
            event = {
                "sensor_id": SENSOR_ID,
                "temperature": round(current_temp, 2),
                "timestamp": datetime.now().isoformat()
            }

            # Publicar no Kafka
            producer.send(TOPIC_RAW_EVENTS, event)
            producer.flush()  # Garante que foi enviado

            # Log formatado
            print(f"[{iteration:04d}] {event['timestamp'][:19]} | 🌡️  {event['temperature']:5.2f}°C")

            # Aguardar próxima leitura
            time.sleep(READING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n" + "-" * 60)
        print("Produtor interrompido pelo usuário")
        print(f"Total de eventos publicados: {iteration}")
        print("=" * 60)
    finally:
        producer.close()
        print("Conexão com Kafka fechada")


if __name__ == "__main__":
    main()
