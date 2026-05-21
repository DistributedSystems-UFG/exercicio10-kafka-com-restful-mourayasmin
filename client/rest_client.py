# Cliente REST
# Consultas exemplo:
# - Última leitura do sensor
# - Média das últimas N horas
# - Dados históricos

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from datetime import datetime, timedelta
from common.config import REST_SERVER_HOST, REST_SERVER_PORT, SENSOR_ID


class TemperatureClient:
    """
    CONCEITO: REST CLIENT

    Este cliente faz requisições HTTP ao servidor REST.
    Usa a biblioteca requests (padrão Python para HTTP).

    PARADIGMA: Cliente-Servidor via HTTP
    - Comunicação síncrona (aguarda resposta)
    - Formato JSON (legível, universal)
    - Protocolo HTTP (ubíquo, firewall-friendly)

    Diferença vs gRPC:
    - REST: requests simples, debugging fácil (curl, Postman)
    - gRPC: requer stub gerado, ferramentas específicas (grpcurl)
    """

    def __init__(self, host, port):
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()  # Reutiliza conexões TCP

    def get_latest_reading(self, sensor_id):
        """Consulta última leitura do sensor."""
        print(f"\nConsultando última leitura do sensor {sensor_id}...")

        url = f"{self.base_url}/sensors/{sensor_id}/latest"

        try:
            response = self.session.get(url)

            if response.status_code == 200:
                data = response.json()

                print("\n" + "=" * 60)
                print("ÚLTIMA LEITURA")
                print("=" * 60)
                print(f"Sensor ID:    {data['sensor_id']}")
                print(f"Temperatura:  {data['temperature']}°C")
                print(f"Timestamp:    {data['timestamp']}")
                print("=" * 60)

            elif response.status_code == 404:
                print("Nenhum dado encontrado para este sensor.")

            else:
                print(f"Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")
            print("Certifique-se de que o servidor REST está rodando.")

    def get_average_stats(self, sensor_id, hours):
        """Consulta estatísticas das últimas N horas."""
        print(f"\nConsultando média das últimas {hours} horas...")

        url = f"{self.base_url}/sensors/{sensor_id}/stats"
        params = {"hours": hours}

        try:
            response = self.session.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                print("\n" + "=" * 60)
                print(f"ESTATÍSTICAS DAS ÚLTIMAS {hours} HORAS")
                print("=" * 60)
                print(f"Sensor ID:          {data['sensor_id']}")
                print(f"Temperatura Média:  {data['avg_temperature']}°C")
                print(f"Total de Amostras:  {data['sample_count']}")
                print(f"Janela Início:      {data['window_start'][:19]}")
                print(f"Janela Fim:         {data['window_end'][:19]}")
                print("=" * 60)

            elif response.status_code == 404:
                print(f"Nenhum dado encontrado para as últimas {hours} horas.")

            else:
                print(f"Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")

    def get_historical_data(self, sensor_id, start_time, end_time):
        """Consulta dados históricos em um período."""
        print(f"\nConsultando histórico de {start_time} até {end_time}...")

        url = f"{self.base_url}/sensors/{sensor_id}/historical"
        params = {
            "start": start_time,
            "end": end_time
        }

        try:
            response = self.session.get(url, params=params)

            if response.status_code == 200:
                result = response.json()
                data = result.get('data', [])

                if data:
                    print("\n" + "=" * 60)
                    print("DADOS HISTÓRICOS")
                    print("=" * 60)
                    print(f"Total de registros: {len(data)}\n")

                    for i, stats in enumerate(data, 1):
                        print(f"[{i}] Média: {stats['avg_temperature']:5.2f}°C | "
                              f"Amostras: {stats['sample_count']:3d} | "
                              f"Janela: {stats['window_start'][:19]} → {stats['window_end'][:19]}")

                    print("=" * 60)
                else:
                    print("Nenhum dado encontrado para este período.")

            elif response.status_code == 404:
                print("Nenhum dado encontrado para este período.")

            else:
                print(f"Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")

    def check_health(self):
        """Verifica se o servidor está saudável."""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=2)
            if response.status_code == 200:
                print("✅ Servidor REST está saudável")
                return True
        except requests.exceptions.RequestException:
            pass

        print("❌ Servidor REST não está acessível")
        return False

    def close(self):
        """Fecha a sessão HTTP."""
        self.session.close()


def print_menu():
    """Exibe menu interativo."""
    print("\n" + "=" * 60)
    print("CLIENTE REST - TEMPERATURE SERVICE")
    print("=" * 60)
    print("1 Ver última leitura do sensor")
    print("2 Ver média das últimas N horas")
    print("3 Ver dados históricos (período específico)")
    print("4 Verificar saúde do servidor")
    print("5 Sair")
    print("=" * 60)


def main():
    """
    Menu interativo para consultas.

    CONCEITO: Este cliente usa HTTP/JSON, que é universal.
    Poderia ser substituído por:
    - curl (linha de comando)
    - Postman (GUI)
    - Navegador (para GETs simples)
    - JavaScript fetch() (frontend web)

    Essa é a força do REST: compatibilidade universal.
    """
    print("\nConectando ao servidor REST...")
    print(f"URL: http://{REST_SERVER_HOST}:{REST_SERVER_PORT}\n")

    client = TemperatureClient(REST_SERVER_HOST, REST_SERVER_PORT)

    # Verificar se servidor está acessível
    if not client.check_health():
        print("\nO servidor REST não está rodando.")
        print("Execute: python3 service/rest_server.py")
        return

    try:
        while True:
            print_menu()
            choice = input("\nEscolha uma opção: ").strip()

            if choice == '1':
                # Última leitura
                client.get_latest_reading(SENSOR_ID)

            elif choice == '2':
                # Média das últimas N horas
                try:
                    hours = int(input("Quantas horas? (ex: 2, 6, 12): "))
                    client.get_average_stats(SENSOR_ID, hours)
                except ValueError:
                    print("Por favor, digite um número válido.")

            elif choice == '3':
                # Dados históricos
                print("\nFormato: YYYY-MM-DD HH:MM:SS (ex: 2026-05-21 10:00:00)")
                start = input("Data/hora início: ").strip()
                end = input("Data/hora fim:    ").strip()

                # Validação simples
                if not start or not end:
                    print("Datas não podem estar vazias.")
                else:
                    # Converter para ISO format se necessário
                    if 'T' not in start:
                        start = start.replace(' ', 'T')
                    if 'T' not in end:
                        end = end.replace(' ', 'T')

                    client.get_historical_data(SENSOR_ID, start, end)

            elif choice == '4':
                # Health check
                client.check_health()

            elif choice == '5':
                print("\nEncerrando cliente...")
                break

            else:
                print("\nOpção inválida. Tente novamente.")

    except KeyboardInterrupt:
        print("\n\nCliente interrompido.")
    finally:
        client.close()
        print("Sessão HTTP fechada.")


if __name__ == "__main__":
    main()
