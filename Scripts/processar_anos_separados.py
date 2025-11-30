import gc
import os

import pandas as pd


def processar_anos_separados():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "Dados")

    # Configuração: Separação explícita dos anos
    config_anos = {
        "2023": ["data_Q1_2023", "data_Q2_2023", "data_Q3_2023", "data_Q4_2023"],
        "2024": ["data_Q1_2024", "data_Q2_2024", "data_Q3_2024", "data_Q4_2024"],
    }

    colunas_necessarias = [
        "serial_number",
        "model",
        "failure",
        "smart_5_raw",
        "smart_198_raw",
    ]

    # --- Loop Principal: Processa um ano de cada vez ---
    for ano, pastas in config_anos.items():
        print(f"\n")
        print(f"INICIANDO PROCESSAMENTO DO ANO {ano}")

        frota = {}  # Reseta o dicionário para cada ano
        arquivo_saida = os.path.join(data_dir, f"tabela_vida_{ano}.csv")

        for nome_pasta in pastas:
            caminho_pasta = os.path.join(data_dir, nome_pasta)
            if not os.path.exists(caminho_pasta):
                print(f"Aviso: Pasta {nome_pasta} não encontrada. Pulando.")
                continue

            arquivos_csv = sorted(
                [f for f in os.listdir(caminho_pasta) if f.endswith(".csv")]
            )

            # Pula pasta se estiver vazia
            if not arquivos_csv:
                continue

            print(f"--> Lendo pasta {nome_pasta} ({len(arquivos_csv)} arquivos)...")

            for i, arquivo in enumerate(arquivos_csv):
                caminho_completo = os.path.join(caminho_pasta, arquivo)

                try:
                    # Lendo apenas o mínimo necessário
                    df = pd.read_csv(
                        caminho_completo, usecols=lambda c: c in colunas_necessarias
                    )

                    # Tratamento de Nulos
                    cols_smart = ["smart_5_raw", "smart_198_raw"]
                    for col in cols_smart:
                        if col not in df.columns:
                            df[col] = 0
                        else:
                            df[col] = df[col].fillna(0)

                    for row in df.itertuples(index=False):
                        serial = row.serial_number

                        if serial not in frota:
                            frota[serial] = {
                                "model": row.model,
                                "falhou": int(row.failure),
                                "dias_ativos": 1,  # Contagem de dias para precisão do AFR anual
                                "max_smart5": float(row.smart_5_raw),
                                "max_smart198": float(row.smart_198_raw),
                            }
                        else:
                            dados = frota[serial]
                            # Incrementa contador de dias (1 dia = 24h de operação no ano)
                            dados["dias_ativos"] += 1

                            # Se falhou, marca
                            if dados["falhou"] == 0 and row.failure == 1:
                                dados["falhou"] = 1

                            # Atualiza SMART (pior caso)
                            if row.smart_5_raw > dados["max_smart5"]:
                                dados["max_smart5"] = float(row.smart_5_raw)
                            if row.smart_198_raw > dados["max_smart198"]:
                                dados["max_smart198"] = float(row.smart_198_raw)

                except Exception as e:
                    print(f"Erro no arquivo {arquivo}: {e}")

                # Feedback visual a cada 10 arquivos
                if i % 10 == 0:
                    print(f"Processando {i}/{len(arquivos_csv)}...", end="\r")

                del df
                gc.collect()

        # Salva o arquivo do ano
        if frota:
            print(f"\nSalvando dados de {ano}...")
            df_final = pd.DataFrame.from_dict(frota, orient="index")
            df_final.index.name = "serial_number"
            df_final.reset_index(inplace=True)

            # Calcula horas baseadas nos dias ativos naquele ano (Crucial para o AFR correto)
            df_final["horas_no_ano"] = df_final["dias_ativos"] * 24

            df_final.to_csv(arquivo_saida, index=False)
            print(f"SUCESSO: {arquivo_saida} gerado com {len(df_final)} discos.")
        else:
            print(f"ERRO: Nenhum dado encontrado para {ano}.")


if __name__ == "__main__":
    processar_anos_separados()
