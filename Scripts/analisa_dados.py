import math
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def analise_final_completa():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "Dados")

    arq_2023 = os.path.join(data_dir, "tabela_vida_2023.csv")
    arq_2024 = os.path.join(data_dir, "tabela_vida_2024.csv")

    if not os.path.exists(arq_2023) or not os.path.exists(arq_2024):
        print("ERRO CRÍTICO: Arquivos de 2023 ou 2024 não encontrados.")
        print("Rode 'processar_anos_separados.py' primeiro.")
        return

    print("Carregando dados...")
    df_23 = pd.read_csv(arq_2023)
    df_24 = pd.read_csv(arq_2024)

    # Adiciona coluna de ano para facilitar a fusão global depois
    df_23["ano_ref"] = 2023
    df_24["ano_ref"] = 2024

    # Gera Relatório Detalhado para um DataFrame
    def gerar_relatorio_ano(df, titulo_ano):
        print(f"\nRESULTADOS DETALHADOS - {titulo_ano}")

        # Totais
        horas = df["horas_no_ano"].sum()
        falhas = df["falhou"].sum()
        discos = len(df)  # Número de registros (discos ativos naquele ano)
        anos_disco = horas / 8760

        # Indicadores Globais
        print(f"Total de Discos Monitorados: {discos}")
        print(f"Tempo Total de Operação:     {horas:,.0f} horas")
        print(f"Total de Falhas Observadas:  {falhas}")

        if falhas > 0:
            mtbf = horas / falhas
            afr = (falhas / anos_disco) if anos_disco > 0 else 0

            # Lambda
            try:
                lamb = -math.log(1 - afr) / 8760 if afr < 1 else 0
            except:
                lamb = 0

            print(f"MTBF (Calculado): {mtbf:,.2f} horas")
            print(f"AFR (Calculado):  {afr:.6f} ({afr * 100:.2f}%)")
            print(f"Taxa λ (via AFR): {lamb:.10f} falhas/hora")
        else:
            print("Sem falhas registradas. AFR = 0%.")

        # Ranking de Modelos
        print(f"RANKING DE MODELOS {titulo_ano} (Critério: Menor AFR)")

        stats = (
            df.groupby("model")
            .agg(
                horas=("horas_no_ano", "sum"),
                falhas=("falhou", "sum"),
                qtd=("serial_number", "count"),
            )
            .reset_index()
        )

        # Filtro de relevância: > 50 unidades e > 0 horas
        stats = stats[(stats["qtd"] > 50) & (stats["horas"] > 0)].copy()

        # AFR por Modelo
        stats["anos_disco_mod"] = stats["horas"] / 8760
        stats["AFR"] = stats.apply(
            lambda x: x["falhas"] / x["anos_disco_mod"]
            if x["anos_disco_mod"] > 0
            else 0,
            axis=1,
        )
        stats["AFR_%"] = stats["AFR"] * 100

        # Ordenação (Menor AFR = Melhor)
        ranking = stats.sort_values(by=["AFR", "horas"], ascending=[True, False]).head(
            10
        )
        print(ranking[["model", "AFR_%", "falhas", "qtd"]].to_string(index=False))

        # Analise SMART (Se colunas existirem e tiver falhas)
        if "max_smart5" in df.columns:
            falhos = df[df["falhou"] == 1]
            saudaveis = df[df["falhou"] == 0]

            if not falhos.empty:
                print(f"\nANÁLISE SMART {titulo_ano}")
                print(
                    f"SMART 5 (Realocados) - Falhos:   {falhos['max_smart5'].mean():.2f} vs Saudáveis: {saudaveis['max_smart5'].mean():.2f}"
                )
                print(
                    f"SMART 198 (Incorrig) - Falhos:   {falhos['max_smart198'].mean():.2f} vs Saudáveis: {saudaveis['max_smart198'].mean():.2f}"
                )

    # COMPARAÇÃO DE EVOLUÇÃO (AFR GLOBAL)
    # (Cálculo rápido só para o delta)
    def get_afr(df):
        h = df["horas_no_ano"].sum()
        f = df["falhou"].sum()
        return (f / (h / 8760)) * 100 if h > 0 else 0

    afr23 = get_afr(df_23)
    afr24 = get_afr(df_24)
    delta = afr24 - afr23

    print("EVOLUÇÃO GLOBAL DA CONFIABILIDADE")
    print(f"AFR 2023: {afr23:.2f}%")
    print(f"AFR 2024: {afr24:.2f}%")
    if delta > 0:
        print(f" ALERTA: O AFR subiu {delta:.2f} p.p. (Piora)")
    else:
        print(f" POSITIVO: O AFR caiu {abs(delta):.2f} p.p. (Melhora)")

    # RELATÓRIOS INDIVIDUAIS (2023 e 2024)
    gerar_relatorio_ano(df_23, "2023")
    gerar_relatorio_ano(df_24, "2024")

    # RANKING GLOBAL (2023 + 2024 CONSOLIDADO)
    print("RANKING GLOBAL CONSOLIDADO (2023 + 2024)")

    # Concatena os dois dataframes
    df_global = pd.concat([df_23, df_24])

    # Agupa por modelo somando horas e falhas dos dois anos
    stats_global = (
        df_global.groupby("model")
        .agg(
            horas_totais=("horas_no_ano", "sum"),
            falhas_totais=("falhou", "sum"),
            # Contagem de registros (discos-ano)
            registros=("serial_number", "count"),
        )
        .reset_index()
    )

    # Filtro de relevância maior para o global (> 100 registros somados)
    stats_global = stats_global[
        (stats_global["registros"] > 100) & (stats_global["horas_totais"] > 0)
    ].copy()

    # Cálculo do AFR Global
    # AFR Global = Total Falhas (2 anos) / (Total Horas (2 anos) / 8760)
    # Nota: Isso dá a taxa média anualizada ponderada pelo tempo de operação
    stats_global["anos_disco_global"] = stats_global["horas_totais"] / 8760
    stats_global["AFR_Global"] = stats_global.apply(
        lambda x: x["falhas_totais"] / x["anos_disco_global"]
        if x["anos_disco_global"] > 0
        else 0,
        axis=1,
    )
    stats_global["AFR_%"] = stats_global["AFR_Global"] * 100

    # Ranking A: Top 10 Mais Confiáveis (Menor AFR)
    ranking_top = stats_global.sort_values(
        by=["AFR_Global", "horas_totais"], ascending=[True, False]
    ).head(10)

    print("TOP 10 MODELOS MAIS CONFIÁVEIS (2023-2024):")
    print(
        ranking_top[["model", "AFR_%", "falhas_totais", "registros"]].to_string(
            index=False
        )
    )

    # Ranking B: Top 5 Piores
    ranking_bottom = (
        stats_global[stats_global["falhas_totais"] > 0]
        .sort_values(by="AFR_Global", ascending=False)
        .head(5)
    )
    print("TOP 5 MODELOS COM MAIOR TAXA DE FALHA (PIORES):")
    print(
        ranking_bottom[["model", "AFR_%", "falhas_totais", "registros"]].to_string(
            index=False
        )
    )

    # Análise SMART Global (2023+2024)
    print("ANÁLISE QUALITATIVA SMART GLOBAL (Média 2023-2024)")
    if "max_smart5" in df_global.columns:
        falhos_g = df_global[df_global["falhou"] == 1]
        saudaveis_g = df_global[df_global["falhou"] == 0]

        if not falhos_g.empty:
            print(
                f"SMART 5 (Realocados) - Falhos:   {falhos_g['max_smart5'].mean():.2f} vs Saudáveis: {saudaveis_g['max_smart5'].mean():.2f}"
            )
            print(
                f"SMART 198 (Incorrig) - Falhos:   {falhos_g['max_smart198'].mean():.2f} vs Saudáveis: {saudaveis_g['max_smart198'].mean():.2f}"
            )

    h_23 = df_23["horas_no_ano"].sum()
    f_23 = df_23["falhou"].sum()
    afr_23 = f_23 / (h_23 / 8760)

    try:
        lamb_23 = -math.log(1 - afr_23) / 8760 if afr_23 < 1 else 0
    except:
        lamb_23 = 0

    if lamb_23 > 0:
        t = np.linspace(0, 43800, 100)  # 5 anos
        R_t_23 = np.exp(-lamb_23 * t)

        plt.figure(figsize=(10, 6))
        plt.plot(
            t,
            R_t_23,
            color="blue",
            lw=2,
            label=f"R(t) 2023 (AFR={afr_23 * 100:.2f}%)",
        )
        if min(R_t_23) > 0.95:
            plt.ylim(0.95, 1.001)
        plt.title("Curva de Confiabilidade - Ano 2023")
        plt.xlabel("Tempo (horas)")
        plt.ylabel("Confiabilidade")
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.legend()
        plt.savefig(os.path.join(data_dir, "Grafico_Rt_2023.png"))
        plt.close()  # Limpa a figura para não sobrepor
        print(f"\n Gráfico R(t) 2023 salvo.")

    h_global = df_global["horas_no_ano"].sum()
    f_global = df_global["falhou"].sum()
    afr_global_avg = f_global / (h_global / 8760)

    try:
        lamb_global = -math.log(1 - afr_global_avg) / 8760 if afr_global_avg < 1 else 0
    except:
        lamb_global = 0

    if lamb_global > 0:
        t = np.linspace(0, 43800, 100)  # 5 anos
        R_t = np.exp(-lamb_global * t)

        plt.figure(figsize=(10, 6))
        plt.plot(
            t,
            R_t,
            color="purple",
            lw=2,
            label=f"R(t) Global 23-24 (AFR={afr_global_avg * 100:.2f}%)",
        )
        if min(R_t) > 0.95:
            plt.ylim(0.95, 1.001)
        plt.title("Curva de Confiabilidade Global (Média 2023-2024)")
        plt.xlabel("Tempo (horas)")
        plt.ylabel("Confiabilidade")
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.legend()
        plt.savefig(os.path.join(data_dir, "Grafico_Rt_Global_23_24.png"))
        plt.close()


if __name__ == "__main__":
    analise_final_completa()
