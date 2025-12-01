import gc
import os

import pandas as pd


def processar_ano_completo():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "Dados")

    # Lista das pastas em ordem cronológica (2023 e 2024)
    pastas_para_processar = [
        "data_Q1_2023",
        "data_Q2_2023",
        "data_Q3_2023",
        "data_Q4_2023",
        "data_Q1_2024",
        "data_Q2_2024",
        "data_Q3_2024",
        "data_Q4_2024",
    ]

    # Atualizamos o nome para refletir que são 2 anos
    arquivo_saida = os.path.join(data_dir, "tabela_vida_2023_2024.csv")

    # Este dicionário vai crescer conforme novos discos aparecem durante o ano
    frota = {}

    colunas_necessarias = [
        "serial_number",
        "model",
        "failure",
        "smart_5_raw",
        "smart_9_raw",
        "smart_198_raw",
    ]

    print(f"Iniciando processamento de {len(pastas_para_processar)} trimestres...")

    for nome_pasta in pastas_para_processar:
        caminho_pasta = os.path.join(data_dir, nome_pasta)

        if not os.path.exists(caminho_pasta):
            print(f"AVISO: Pasta não encontrada: {nome_pasta}. Pulando...")
            continue

        # Lista arquivos CSV dentro desta pasta específica
        arquivos_csv = sorted(
            [
                f
                for f in os.listdir(caminho_pasta)
                if f.endswith(".csv") and not f.startswith(".")
            ]
        )

        if not arquivos_csv:
            print(f"AVISO: Pasta vazia: {nome_pasta}. Pulando...")
            continue

        print(f"\n---> Entrando na pasta: {nome_pasta} ({len(arquivos_csv)} arquivos)")

        for i, arquivo in enumerate(arquivos_csv):
            caminho_completo = os.path.join(caminho_pasta, arquivo)

            # Feedback visual simples para não poluir o terminal
            if i % 5 == 0:
                print(
                    f"Processando {nome_pasta}: {i + 1}/{len(arquivos_csv)}...",
                    end="\r",
                )

            try:
                # Leitura Otimizada (Mesma lógica do seu código anterior)
                df = pd.read_csv(
                    caminho_completo, usecols=lambda c: c in colunas_necessarias
                )

                # Tratamento de Nulos e Colunas Faltantes
                cols_smart = ["smart_5_raw", "smart_9_raw", "smart_198_raw"]
                for col in cols_smart:
                    if col not in df.columns:
                        df[col] = 0
                    else:
                        df[col] = df[col].fillna(0)

                # Atualização do Dicionário (Itertuples = Alta Performance)
                for row in df.itertuples(index=False):
                    serial = row.serial_number

                    # Se o HD é novo (ainda não estava no dicionário)
                    if serial not in frota:
                        frota[serial] = {
                            "model": row.model,
                            "falhou": int(row.failure),
                            "horas_smart9": float(row.smart_9_raw),
                            "max_smart5": float(row.smart_5_raw),
                            "max_smart198": float(row.smart_198_raw),
                        }
                    else:
                        # Se já existe, atualizamos o estado
                        dados = frota[serial]

                        # Falha é um estado absorvente (se falhou uma vez, falhou para sempre)
                        if dados["falhou"] == 0 and row.failure == 1:
                            dados["falhou"] = 1

                        # Atualiza horas (sempre pegamos o maior valor, pois o tempo só avança)
                        if row.smart_9_raw > dados["horas_smart9"]:
                            dados["horas_smart9"] = float(row.smart_9_raw)

                        # Atualiza métricas de saúde (pior caso observado)
                        if row.smart_5_raw > dados["max_smart5"]:
                            dados["max_smart5"] = float(row.smart_5_raw)
                        if row.smart_198_raw > dados["max_smart198"]:
                            dados["max_smart198"] = float(row.smart_198_raw)

            except Exception as e:
                print(f"\nErro ao ler {arquivo}: {e}")

            # Limpeza de Memória OBRIGATÓRIA a cada arquivo
            del df
            gc.collect()

        print(f"\nConcluído pasta {nome_pasta}.")

    print("\n" + "-" * 40)
    print("Consolidando dados de 2024...")

    if not frota:
        print("ERRO: Nenhum dado foi processado em nenhuma pasta.")
        return

    # Converte dicionário para DataFrame
    df_final = pd.DataFrame.from_dict(frota, orient="index")
    df_final.index.name = "serial_number"
    df_final.reset_index(inplace=True)

    print(f"Total de discos únicos monitorados em 2024: {len(df_final)}")
    print(f"Salvando tabela consolidada em: {arquivo_saida}")

    df_final.to_csv(arquivo_saida, index=False)
    print("SUCESSO! O arquivo está pronto para análise.")


if __name__ == "__main__":
    processar_ano_completo()
