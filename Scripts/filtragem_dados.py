import gc
import os

import pandas as pd


def processar_pasta_completa():
    # --- 1. Configuração de Caminhos ---
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Caminho da pasta onde estão os 30 arquivos CSV
    pasta_arquivos = os.path.join(os.path.dirname(script_dir), "Dados", "data_Q3_2025")

    # Caminho onde vamos salvar o resultado final
    arquivo_saida = os.path.join(
        os.path.dirname(script_dir), "Dados", "tabela_vida_final.csv"
    )

    # Verifica se a pasta existe
    if not os.path.exists(pasta_arquivos):
        print(f"ERRO: A pasta não foi encontrada: {pasta_arquivos}")
        print("Verifique se o nome da pasta é exatamente 'data_Q3_2025'.")
        return

    # Lista os arquivos CSV ordenados
    arquivos_csv = sorted(
        [
            f
            for f in os.listdir(pasta_arquivos)
            if f.endswith(".csv") and not f.startswith(".")
        ]
    )

    if not arquivos_csv:
        print(f"ERRO: Nenhum arquivo CSV encontrado dentro de: {pasta_arquivos}")
        return

    print(f"Encontrados {len(arquivos_csv)} arquivos. Iniciando processamento...")

    # --- 2. Dicionário de Agregação (Memória) ---
    # Guardaremos apenas o resumo de cada HD aqui para economizar RAM
    frota = {}

    # Colunas que precisamos carregar
    colunas_necessarias = [
        "serial_number",
        "model",
        "failure",
        "smart_5_raw",
        "smart_9_raw",
        "smart_198_raw",
    ]

    # --- 3. Loop pelos 30 dias ---
    for i, arquivo in enumerate(arquivos_csv):
        caminho_completo = os.path.join(pasta_arquivos, arquivo)
        print(f"[{i + 1}/{len(arquivos_csv)}] Processando: {arquivo}...")

        try:
            # Lê o arquivo do dia (apenas colunas úteis)
            # 'usecols' filtra na leitura, economizando muita memória
            df = pd.read_csv(
                caminho_completo, usecols=lambda c: c in colunas_necessarias
            )

            # Garante que as colunas SMART existam (preenche com 0 se faltar)
            cols_smart = ["smart_5_raw", "smart_9_raw", "smart_198_raw"]
            for col in cols_smart:
                if col not in df.columns:
                    df[col] = 0
                else:
                    df[col] = df[col].fillna(0)

            # Loop otimizado linha a linha usando itertuples (muito mais rápido que iterrows)
            for row in df.itertuples(index=False):
                serial = row.serial_number

                # Se é a primeira vez que vemos este HD
                if serial not in frota:
                    frota[serial] = {
                        "model": row.model,
                        "falhou": int(row.failure),
                        "horas_smart9": float(row.smart_9_raw),
                        "max_smart5": float(row.smart_5_raw),
                        "max_smart198": float(row.smart_198_raw),
                    }
                else:
                    # Se já conhecemos, atualizamos os dados
                    dados = frota[serial]

                    # Se ele já tinha falhado antes, mantém o estado de falha
                    if dados["falhou"] == 0 and row.failure == 1:
                        dados["falhou"] = 1

                    # Atualiza horas para o maior valor encontrado (o tempo avança)
                    if row.smart_9_raw > dados["horas_smart9"]:
                        dados["horas_smart9"] = float(row.smart_9_raw)

                    # Guarda o pior estado de saúde observado
                    if row.smart_5_raw > dados["max_smart5"]:
                        dados["max_smart5"] = float(row.smart_5_raw)
                    if row.smart_198_raw > dados["max_smart198"]:
                        dados["max_smart198"] = float(row.smart_198_raw)

        except Exception as e:
            print(f"--> Erro ao ler {arquivo}: {e}")

        # --- Limpeza Crítica de Memória ---
        # Deleta o dataframe do dia e força o Python a limpar a RAM
        del df
        gc.collect()

    # --- 4. Salvando Resultado ---
    print("-" * 40)
    print("Consolidando dados finais...")

    if not frota:
        print("Nenhum dado foi processado.")
        return

    # Transforma dicionário em DataFrame
    df_final = pd.DataFrame.from_dict(frota, orient="index")
    df_final.index.name = "serial_number"
    df_final.reset_index(inplace=True)

    print(f"Total de discos únicos encontrados: {len(df_final)}")
    print(f"Salvando em: {arquivo_saida}")

    df_final.to_csv(arquivo_saida, index=False)
    print("SUCESSO! Tabela de vida gerada.")


if __name__ == "__main__":
    processar_pasta_completa()
