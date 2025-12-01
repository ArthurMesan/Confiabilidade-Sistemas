import math
import os

import numpy as np
import pandas as pd

# Ajuste de largura para exibição no pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


def formatar_tabela(df, titulo):
    """
    Imprime uma tabela formatada com colunas fixas para evitar desalinhamento.
    """
    print(f"\n>>> {titulo}")

    print(
        f"{'MODELO DO DISCO':<35} | {'AFR (%)':<10} | {'FALHAS':<8} | {'QTD DISCOS':<12}"
    )

    for _, row in df.iterrows():
        modelo = str(row["model"])
        if len(modelo) > 33:
            modelo = modelo[:30] + "..."

        afr_val = float(row["AFR_%"])
        falhas_val = int(row["falhas"])
        qtd_val = int(row["qtd"])

        print(f"{modelo:<35} | {afr_val:<10.4f} | {falhas_val:<8} | {qtd_val:<12}")


def gerar_relatorio_texto(df, nome_periodo):
    print(f"\n")
    print(f" RELATÓRIO DETALHADO: {nome_periodo}")

    # 1. Totais
    horas_totais = df["horas_no_ano"].sum()
    falhas_totais = df["falhou"].sum()
    total_discos = len(df)

    # Indicadores Globais
    # AFR (decimal) para cálculos = Falhas / (Horas / 8760)
    anos_disco = horas_totais / 8760
    afr_decimal = (falhas_totais / anos_disco) if anos_disco > 0 else 0
    afr_percentual = afr_decimal * 100

    mtbf_medio = (horas_totais / falhas_totais) if falhas_totais > 0 else 0

    # Cálculo de R(t) e Lambda
    # Lambda = -ln(1 - AFR) / 8760 (Considerando AFR como probabilidade anual)
    # nota: Se AFR for pequeno, Lambda ~= AFR / 8760
    try:
        # Usamos o AFR decimal (ex: 0.0173 para 1.73%)
        if afr_decimal < 1:
            lamb = -math.log(1 - afr_decimal) / 8760
        else:
            lamb = 0  # Se AFR >= 100%, sistema falha com certeza
    except:
        lamb = 0

    # Função R(t) = exp(-lambda * t)
    def calcular_Rt(t_horas):
        return math.exp(-lamb * t_horas)

    print(f"\n[RESUMO DA FROTA]")
    print(f" Total de Discos:   {total_discos:,}".replace(",", "."))
    print(f" Horas Operação:    {horas_totais:,.0f}".replace(",", "."))
    print(f" Total de Falhas:   {falhas_totais}")
    print(
        f"• MTBF Médio:        {mtbf_medio:,.2f} horas".replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )
    print(f" AFR Médio:         {afr_percentual:.4f}%")
    print(f" Taxa de Falha (λ): {lamb:.10f} falhas/hora")

    print(f"\n[VALORES DE CONFIABILIDADE R(t)]")
    print(f"Probabilidade do sistema NÃO falhar após:")
    print(f"1 Dia (24h):    {calcular_Rt(24) * 100:.6f}%")
    print(f"1 Mês (730h):   {calcular_Rt(730) * 100:.6f}%")
    print(f"6 Meses (4380h):{calcular_Rt(4380) * 100:.4f}%")
    print(f"1 Ano (8760h):  {calcular_Rt(8760) * 100:.4f}%")

    # Preparação para Rankings
    stats = (
        df.groupby("model")
        .agg(
            horas=("horas_no_ano", "sum"),
            falhas=("falhou", "sum"),
            qtd=("serial_number", "count"),
        )
        .reset_index()
    )

    # Filtro: Apenas modelos com > 50 unidades e alguma operação
    stats = stats[(stats["qtd"] > 50) & (stats["horas"] > 0)].copy()

    # Cálculo do AFR por modelo (sempre em %)
    stats["anos_disco_mod"] = stats["horas"] / 8760
    stats["AFR_%"] = stats.apply(
        lambda x: (x["falhas"] / x["anos_disco_mod"] * 100)
        if x["anos_disco_mod"] > 0
        else 0,
        axis=1,
    )

    # Rankings
    # Melhores
    melhores = stats.sort_values(by=["AFR_%", "horas"], ascending=[True, False]).head(
        10
    )
    formatar_tabela(melhores, f"TOP 10 MODELOS MAIS CONFIÁVEIS ({nome_periodo})")

    # Piores
    piores = stats.sort_values(by=["AFR_%"], ascending=False).head(5)
    piores = piores[piores["falhas"] > 0]

    if not piores.empty:
        formatar_tabela(piores, f"TOP 5 MAIORES TAXAS DE FALHA ({nome_periodo})")
    else:
        print("\n[!] Nenhum modelo apresentou falhas significativas neste grupo.")

    # Análise SMART (Resumida)
    if "max_smart5" in df.columns:
        falhos = df[df["falhou"] == 1]
        saudaveis = df[df["falhou"] == 0]
        if not falhos.empty:
            print(f"\n[MÉDIAS DE ATRIBUTOS S.M.A.R.T]")
            print(f"Setores Realocados (SMART 5):")
            print(f"Discos Saudáveis: {saudaveis['max_smart5'].mean():.2f}")
            print(f"Discos Falhos:    {falhos['max_smart5'].mean():.2f}")


def analise_final_completa():
    # Caminhos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "Dados")
    arq_2023 = os.path.join(data_dir, "tabela_vida_2023.csv")
    arq_2024 = os.path.join(data_dir, "tabela_vida_2024.csv")

    if not os.path.exists(arq_2023) or not os.path.exists(arq_2024):
        print("ERRO: Arquivos de dados não encontrados na pasta 'Dados'.")
        return

    print("Carregando bases de dados... (Isso pode levar alguns segundos)")
    df_23 = pd.read_csv(arq_2023)
    df_24 = pd.read_csv(arq_2024)

    # Consolidação Global
    df_global = pd.concat([df_23, df_24])

    # Executa os relatórios
    gerar_relatorio_texto(df_23, "ANO 2023")
    gerar_relatorio_texto(df_24, "ANO 2024")
    gerar_relatorio_texto(df_global, "GLOBAL (ACUMULADO 2023-2024)")

    print("\nProcessamento finalizado com sucesso.")


if __name__ == "__main__":
    analise_final_completa()
