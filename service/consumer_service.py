# Consumer Service - Kafka → Database
# Consome eventos do Kafka e persiste no SQLite

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
import json
from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW_EVENTS,
    TOPIC_PROCESSED_EVENTS
)
from service.storage import TemperatureStorage


class ConsumerService:
    """
    CONCEITO: PERSISTÊNCIA DE DADOS

    Este consumidor é responsável por "materializar" os eventos
    do Kafka em um banco de dados permanente.

    Por que precisamos disso?
    - Kafka retém mensagens por tempo limitado (configurável)
    - SQLite permite consultas complexas (JOIN, filtros, etc.)
    - gRPC Server vai consultar o banco, não o Kafka

    PARADIGMA: Event Sourcing + Materialização
    - Eventos são a "fonte da verdade" (Kafka)
    - Banco é uma "visão materializada" dos eventos
    """

    def __init__(self, storage):
        self.storage = storage

        # Consumer para eventos processados (estatísticas)
        self.processed_consumer = KafkaConsumer(
            TOPIC_PROCESSED_EVENTS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Consome desde o início (histórico)
            group_id='storage-group'
        )

        # Consumer para eventos brutos (opcional, mas útil para debug)
        self.raw_consumer = KafkaConsumer(
            TOPIC_RAW_EVENTS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='storage-raw-group'
        )

    def consume_processed_events(self):
        """
        Consome estatísticas processadas e salva no banco.

        CONCEITO: Este é o caminho principal dos dados processados
        para o banco. O gRPC vai consultar estas estatísticas.
        """
        print("=" * 70)
        print("CONSUMER SERVICE - PERSISTÊNCIA NO BANCO")
        print("=" * 70)
        print(f"Consumindo: {TOPIC_PROCESSED_EVENTS}")
        print(f"Salvando em: SQLite (temperature.db)")
        print("=" * 70)
        print("\nCONCEITO: Eventos do Kafka são gravados no banco.")
        print("   Isso permite consultas históricas via gRPC.\n")
        print("-" * 70)

        try:
            for message in self.processed_consumer:
                event = message.value

                # Salvar no banco
                self.storage.insert_stats(
                    sensor_id=event['sensor_id'],
                    avg_temperature=event['avg_temperature'],
                    window_start=event['window_start'],
                    window_end=event['window_end'],
                    sample_count=event['sample_count']
                )

                print(f"Salvo: Média {event['avg_temperature']:5.2f}°C | "
                      f"Amostras: {event['sample_count']:3d} | "
                      f"Até: {event['window_end'][:19]}")

        except KeyboardInterrupt:
            print("\n" + "-" * 70)
            print("Consumer Service interrompido")
            print("=" * 70)
        finally:
            self.processed_consumer.close()
            print("Conexão fechada")

    def consume_raw_events(self):
        """
        Consome eventos brutos e salva no banco.

        CONCEITO: Opcional, mas útil para:
        - Ver todas as leituras individuais
        - Debug e auditoria
        - Reprocessamento futuro
        """
        print("=" * 70)
        print("CONSUMER SERVICE - LEITURAS BRUTAS")
        print("=" * 70)
        print(f"Consumindo: {TOPIC_RAW_EVENTS}")
        print(f"Salvando em: SQLite (raw_readings)")
        print("=" * 70)

        try:
            for message in self.raw_consumer:
                event = message.value

                # Salvar no banco
                self.storage.insert_reading(
                    sensor_id=event['sensor_id'],
                    temperature=event['temperature'],
                    timestamp=event['timestamp']
                )

                print(f"Salvo: {event['timestamp'][:19]} | {event['temperature']:5.2f}°C")

        except KeyboardInterrupt:
            print("\n" + "-" * 70)
            print("Consumer Service interrompido")
            print("=" * 70)
        finally:
            self.raw_consumer.close()
            print("Conexão fechada")


def main():
    """
    Ponto de entrada.

    Por padrão, consome apenas eventos processados (estatísticas).
    Se quiser salvar leituras brutas também, descomente a segunda linha.
    """
    storage = TemperatureStorage()
    service = ConsumerService(storage)

    print("\n Iniciando Consumer Service...")
    print("   (Pressione Ctrl+C para parar)\n")

    # Consumir estatísticas processadas (principal)
    service.consume_processed_events()

    # Descomentar para salvar também leituras brutas:
    # service.consume_raw_events()


if __name__ == "__main__":
    main()
