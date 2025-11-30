import os

import pandas as pd


def filtrar_arquivo_unico():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "Dados")

    # Procura arquivos CSV na pasta Dados que NÃO tenham 'filtrado' no nome
    arquivos_na_pasta = [
        f for f in os.listdir(data_dir) if f.endswith(".csv") and "filtrado" not in f
    ]

    if not arquivos_na_pasta:
        print(f"Nenhum arquivo .csv encontrado na pasta: {data_dir}")
        return

    # Pega o primeiro arquivo encontrado
    nome_arquivo_entrada = arquivos_na_pasta[0]
    caminho_entrada = os.path.join(data_dir, nome_arquivo_entrada)
    caminho_saida = os.path.join(data_dir, f"filtrado_{nome_arquivo_entrada}")

    print(f"Arquivo encontrado: {nome_arquivo_entrada}")
    print(f"Processando...")

    # colunas Necessárias
    colunas_desejadas = [
        "date",
        "serial_number",
        "model",
        "capacity_bytes",
        "failure",
        "smart_5_raw",
        "smart_9_raw",
        "smart_198_raw",
    ]

    try:
        # Lê apenas as colunas desejadas
        df = pd.read_csv(caminho_entrada, usecols=lambda c: c in colunas_desejadas)

        df["date"] = pd.to_datetime(df["date"])

        # Preenche zeros nas colunas SMART (caso existam)
        cols_smart = ["smart_5_raw", "smart_9_raw", "smart_198_raw"]
        cols_existentes = [c for c in cols_smart if c in df.columns]

        if cols_existentes:
            df[cols_existentes] = df[cols_existentes].fillna(0)

        print(f"Salvando arquivo filtrado...")
        df.to_csv(caminho_saida, index=False)

        print("-" * 30)
        print(f"SUCESSO! Arquivo salvo em:\n{caminho_saida}")
        print(f"Linhas processadas: {len(df)}")
        print("-" * 30)

    except Exception as e:
        print(f"Ocorreu um erro: {e}")


if __name__ == "__main__":
    filtrar_arquivo_unico()
