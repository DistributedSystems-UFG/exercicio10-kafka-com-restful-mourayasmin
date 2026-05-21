# Camada de armazenamento - SQLite
# Tabelas:
# - raw_readings: leituras brutas do sensor
# - processed_stats: estatísticas processadas (médias)

import sqlite3
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import DATABASE_PATH


class TemperatureStorage:
    """
    Gerencia armazenamento SQLite para dados de temperatura.

    CONCEITO: Esta classe abstrai o acesso ao banco de dados,
    seguindo o padrão Repository. Componentes que precisam
    de dados apenas chamam métodos desta classe, sem SQL direto.
    """

    def __init__(self, db_path=DATABASE_PATH):
        self.db_path = db_path
        self._create_tables()

    def _get_connection(self):
        """Cria conexão com o banco SQLite."""
        return sqlite3.connect(self.db_path)

    def _create_tables(self):
        """Cria as tabelas se não existirem."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Tabela para leituras brutas do sensor
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''')

        # Tabela para estatísticas processadas (médias móveis)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                avg_temperature REAL NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                sample_count INTEGER NOT NULL
            )
        ''')

        conn.commit()
        conn.close()

    # === INSERÇÕES ===

    def insert_reading(self, sensor_id, temperature, timestamp):
        """Insere uma leitura bruta do sensor."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO raw_readings (sensor_id, temperature, timestamp) VALUES (?, ?, ?)',
            (sensor_id, temperature, timestamp)
        )
        conn.commit()
        conn.close()

    def insert_stats(self, sensor_id, avg_temperature, window_start, window_end, sample_count):
        """Insere estatísticas processadas (média móvel)."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''INSERT INTO processed_stats
               (sensor_id, avg_temperature, window_start, window_end, sample_count)
               VALUES (?, ?, ?, ?, ?)''',
            (sensor_id, avg_temperature, window_start, window_end, sample_count)
        )
        conn.commit()
        conn.close()

    # === CONSULTAS ===

    def get_latest_reading(self, sensor_id):
        """
        Retorna a última leitura do sensor.
        CONCEITO: Útil para o cliente ver o valor atual em tempo real.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''SELECT sensor_id, temperature, timestamp
               FROM raw_readings
               WHERE sensor_id = ?
               ORDER BY id DESC LIMIT 1''',
            (sensor_id,)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            return {
                'sensor_id': result[0],
                'temperature': result[1],
                'timestamp': result[2]
            }
        return None

    def get_avg_stats(self, sensor_id, hours):
        """
        Retorna estatísticas das últimas N horas.
        CONCEITO: Permite visualizar tendências (média subindo/descendo).
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Calcular timestamp de corte (agora - N horas)
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()

        cursor.execute(
            '''SELECT sensor_id, avg_temperature, window_start, window_end, sample_count
               FROM processed_stats
               WHERE sensor_id = ? AND window_end >= ?
               ORDER BY window_end DESC''',
            (sensor_id, cutoff)
        )

        results = cursor.fetchall()
        conn.close()

        stats = []
        for row in results:
            stats.append({
                'sensor_id': row[0],
                'avg_temperature': row[1],
                'window_start': row[2],
                'window_end': row[3],
                'sample_count': row[4]
            })

        return stats

    def get_historical_data(self, sensor_id, start_time, end_time):
        """
        Retorna estatísticas em um período específico.
        CONCEITO: Permite análise histórica (ex: "como estava na semana passada?").
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''SELECT sensor_id, avg_temperature, window_start, window_end, sample_count
               FROM processed_stats
               WHERE sensor_id = ? AND window_end >= ? AND window_start <= ?
               ORDER BY window_end ASC''',
            (sensor_id, start_time, end_time)
        )

        results = cursor.fetchall()
        conn.close()

        stats = []
        for row in results:
            stats.append({
                'sensor_id': row[0],
                'avg_temperature': row[1],
                'window_start': row[2],
                'window_end': row[3],
                'sample_count': row[4]
            })

        return stats


# Teste rápido (executar apenas se rodado diretamente)
if __name__ == "__main__":
    print("Testando storage...")
    storage = TemperatureStorage('test.db')

    # Inserir leitura de teste
    storage.insert_reading('sensor-001', 25.5, datetime.now().isoformat())
    print("Leitura inserida")

    # Buscar última leitura
    reading = storage.get_latest_reading('sensor-001')
    print(f"Última leitura: {reading}")

    # Limpar teste
    import os
    os.remove('test.db')
    print("Storage funcionando!")
