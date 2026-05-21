# Processador - Consumidor/Produtor
# Consome de: raw-temperature-events
# Processa: calcula média móvel das últimas 2 horas
# Publica em: processed-temperature-events

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timedelta
from collections import deque
from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW_EVENTS,
    TOPIC_PROCESSED_EVENTS,
    SENSOR_ID
)


class TemperatureProcessor:
    """
    CONCEITO: PROCESSAMENTO DE STREAM

    Este componente é híbrido: Consumer + Producer.
    Ele consome eventos brutos, processa (calcula média),
    e produz novos eventos com as estatísticas.

    JANELA DESLIZANTE (Sliding Window):
    Mantém em memória apenas as leituras das últimas 2 horas.
    A cada nova leitura:
    1. Remove leituras antigas (> 2h)
    2. Adiciona nova leitura
    3. Calcula média de todas que sobraram
    """

    def __init__(self, window_hours=2):
        self.window_hours = window_hours
        # Deque é eficiente para adicionar/remover nas extremidades
        self.readings = deque()

        # Consumer: lê do tópico de eventos brutos
        self.consumer = KafkaConsumer(
            TOPIC_RAW_EVENTS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',  # Apenas mensagens novas
            group_id='processor-group'   # Consumer group (permite paralelismo)
        )

        # Producer: publica estatísticas processadas
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _remove_old_readings(self):
        """
        Remove leituras mais antigas que a janela de tempo.

        CONCEITO: Janela deslizante "esquece" dados antigos
        para manter apenas o período relevante.
        """
        cutoff_time = datetime.now() - timedelta(hours=self.window_hours)

        while self.readings and datetime.fromisoformat(self.readings[0]['timestamp']) < cutoff_time:
            self.readings.popleft()  # Remove da esquerda (mais antiga)

    def _calculate_stats(self):
        """
        Calcula estatísticas da janela atual.

        CONCEITO: Agregação de dados em tempo real.
        Em vez de guardar todas as leituras individuais,
        guardamos apenas a média - economiza espaço.
        """
        if not self.readings:
            return None

        temperatures = [r['temperature'] for r in self.readings]
        avg_temp = sum(temperatures) / len(temperatures)

        # Janela de tempo
        window_start = self.readings[0]['timestamp']
        window_end = self.readings[-1]['timestamp']

        return {
            'sensor_id': SENSOR_ID,
            'avg_temperature': round(avg_temp, 2),
            'window_start': window_start,
            'window_end': window_end,
            'sample_count': len(self.readings)
        }

    def process_events(self):
        """
        Loop principal: consome eventos, processa, publica.

        CONCEITO: Pipeline de dados em tempo real
        Entrada → Processamento → Saída (tudo em streaming)
        """
        print("=" * 70)
        print("PROCESSADOR DE TEMPERATURA - JANELA DESLIZANTE")
        print("=" * 70)
        print(f"Consumindo de: {TOPIC_RAW_EVENTS}")
        print(f"Publicando em: {TOPIC_PROCESSED_EVENTS}")
        print(f"Janela: {self.window_hours} horas")
        print("=" * 70)
        print("\n CONCEITO: Cada evento bruto é adicionado à janela.")
        print("   Eventos > 2h são removidos. Média é recalculada e publicada.\n")
        print("-" * 70)

        try:
            for message in self.consumer:
                event = message.value

                # 1. Adicionar nova leitura
                self.readings.append(event)

                # 2. Remover leituras antigas (> 2h)
                self._remove_old_readings()

                # 3. Calcular estatísticas
                stats = self._calculate_stats()

                if stats:
                    # 4. Publicar estatísticas
                    self.producer.send(TOPIC_PROCESSED_EVENTS, stats)
                    self.producer.flush()

                    # Log
                    print(f"Média: {stats['avg_temperature']:5.2f}°C | "
                          f"Amostras: {stats['sample_count']:3d} | "
                          f"Janela: {stats['window_start'][:19]} → {stats['window_end'][:19]}")

        except KeyboardInterrupt:
            print("\n" + "-" * 70)
            print("Processador interrompido")
            print("=" * 70)
        finally:
            self.consumer.close()
            self.producer.close()
            print("Conexões fechadas")


def main():
    processor = TemperatureProcessor(window_hours=2)
    processor.process_events()


if __name__ == "__main__":
    main()
