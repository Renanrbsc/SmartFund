import pandas as pd
import os

def join_dataframes_from_directory(directory):
    dfs = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)
                df = pd.read_csv(
                    filepath,
                    sep=";",
                    encoding="ISO-8859-1",
                    skiprows=[0],
                    decimal=",",
                    dtype=str,
                )
                dfs.append(df)
    if len(dfs) > 0:
        df_result = pd.concat(dfs, ignore_index=True)
        return df_result
    else:
        return None


def definir_compra_venda(row):
    if (row["Quantidade Compra"] > 0) & (row["Quantidade Venda"] == 0):
        row["Tipo Negociacao"] = "Compra"
        row["Preco Compra Atual"] = row["Preco"]
        row["Preco Venda Atual"] = 0
    elif (row["Quantidade Venda"] > 0) & (row["Quantidade Compra"] == 0):
        row["Tipo Negociacao"] = "Venda"
        row["Preco Venda Atual"] = row["Preco"]
        row["Preco Compra Atual"] = 0
    else:
        row["Tipo Negociacao"] = None
        row["Preco Compra Atual"] = None
        row["Preco Venda Atual"] = None
    return row


def validador_estrategia_trade(group):
    # Função para auxiliar na separação de Estrategias Day Trade e Long Trade
    # Necessita de ajuste manual após validação, 
    # deve separar as quantidades que pertencem determinada estrategia
    # Ex: quantidade venda maior que quantidade compra,
    #     copiar linha e dividir o valor pelas quantidades de cada estrategia.
    #     linha 1 priorizar a venda das quantidades Day Trade.
    #     linha 2 separar quantidades restantes como Long Trade.
    group['Tipo Estrategia'] = "Long Trade"
    tem_venda = (group['Quantidade Venda'] != '0').any()
    tem_compra = (group['Quantidade Compra'] != '0').any()
    if tem_venda and tem_compra:
        group['Verificar Day Trade'] = True        
    else:
        group['Verificar Day Trade'] = False
    return group


def calcular_ordem_negociacoes_por_ativo(group):
    """
    Adiciona a ordem de negociaçõs dos ativos por grupos.
    """
    group['Ordem Negociacao'] = range(1, len(group) + 1)
    return group


def calcular_investimento_preco_medio_atual(group):
    # Função para calcular o preço médio acumulado em cada grupo
    investimento_acumulado = 0
    preco_medio = 0
    
    for index, row in group.iterrows():
        if row['Quantidade Compra'] > 0:
            investimento_acumulado += row['Valor Compra']
            preco_medio = investimento_acumulado / row["Quantidade Total Atual"]

            group.at[index, 'Investimento Atual'] = investimento_acumulado
            group.at[index, 'Preco Medio Atual'] = preco_medio
            
        elif row['Quantidade Venda'] > 0:
            valor_investido = preco_medio * row['Quantidade Venda']
            investimento_acumulado -= valor_investido
            
            if row["Quantidade Total Atual"] > 0:
                preco_medio = investimento_acumulado / row["Quantidade Total Atual"]
                
            group.at[index, 'Investimento Atual'] = investimento_acumulado
            group.at[index, 'Preco Medio Atual'] = preco_medio
        else:
            group.at[index, 'Investimento Atual'] = 0
            group.at[index, 'Preco Medio Atual'] = 0
    return group


def calcular_lucro_prejuizo(row):
    # Calcula o Lucro em determinados momentos (Parcial ou inteiro de venda)
    if row["Quantidade Venda"] > 0:
        row["Lucro/Prejuizo"] = -(row["Preco Medio Anterior"] * row["Quantidade Venda"]) + row["Valor Venda"]
    elif row["Quantidade Venda"] == 0:
        row["Lucro/Prejuizo"] = 0

    if row["Lucro/Prejuizo"] > 0:
        row["Lucro Bruto"] = row["Lucro/Prejuizo"]
        row["Prejuizo Bruto"] = 0
    elif row["Lucro/Prejuizo"] < 0:
        row["Prejuizo Bruto"] = row["Lucro/Prejuizo"]
        row["Lucro Bruto"] = 0
    else:
        row["Lucro Bruto"] = 0
        row["Prejuizo Bruto"] = 0

    return row

def calcular_lucro_compensando_prejuizo(group):
    # Função para calcular o lucro liquido bruto que sera necessario para dedução dos impostos
    # apenas as negociacoes com valor positivo no liquido deverao ter imposto.
    prejuizo_acumulado = 0
    for index, row in group.iterrows():
        if row['Tipo Negociacao'] == "Venda":
            if row["Lucro/Prejuizo"] <= 0:
                prejuizo_acumulado += row["Prejuizo Bruto"]
                group.at[index, "Lucro Bruto Compensado"] = 0
                group.at[index, "Prejuizo Acumulado"] = prejuizo_acumulado
            else:
                lucro_compensado = row["Lucro Bruto"] + prejuizo_acumulado
                if lucro_compensado >= 0:
                    prejuizo_acumulado = 0
                    group.at[index, "Lucro Bruto Compensado"] = lucro_compensado
                    group.at[index, "Prejuizo Acumulado"] = prejuizo_acumulado
                else:
                    prejuizo_acumulado = lucro_compensado
                    group.at[index, "Lucro Bruto Compensado"] = 0 
                    group.at[index, "Prejuizo Acumulado"] = prejuizo_acumulado
        else:
            group.at[index, "Lucro Bruto Compensado"] = 0
            group.at[index, "Prejuizo Acumulado"] = 0
    return group


def definir_taxa_imposto_renda_dedo_duro_por_ativo(row):
    # Calcula a taxa de IR (Imposto de Renda) considerando 15% para ações e 20% para FIIs.
    # Ações IR: Day Trade 20%, Swing Trade 15% e Long Trade 15%. 
    # Calcular Dedo Duro da mesma forma, 
    # Dedo Duro: Day Trade 1%, Swing Trade 0.005% e Long Trade 0.005%
    if row["Tipo Negociacao"] == "Venda":
        if row["Tipo Estrategia"] == "Day Trade":
            row["Dedo Duro"] = row["Lucro Bruto"] * 0.01
            row["Taxa IR"] = 0.20
        elif row["Tipo Estrategia"] == "Long Trade":
            if row["Tipo Ativo"] == "Fii":
                row["Dedo Duro"] = row["Lucro Bruto"] * 0.00005
                row["Taxa IR"] = 0.20
            elif row["Tipo Ativo"] == "Acao":
                row["Dedo Duro"] = row["Lucro Bruto"] * 0.00005
                row["Taxa IR"] = 0.15
            else: 
                row["Dedo Duro"] = "Outras Categorias"
                row["Taxa IR"] = "Outras Categorias"
        else:
            row["Dedo Duro"] = "Outras Estrategias"
            row["Taxa IR"] = "Outras Estrategias"
    else:
        row["Dedo Duro"] = 0
        row["Taxa IR"] = 0
    return row


def calcular_imposto_de_renda(row):
    # Calcula o valor do IR para cada transação
    # e desconta o "dedo duro" já pago nas transaçoes
    if row["Lucro Bruto"] > 0:
        row["Imposto Renda Normal"] = row["Lucro Bruto"] * row["Taxa IR"]
        row["Imposto Renda Normal Final"] = row["Imposto Renda Normal"] - row["Dedo Duro"]
    else:
        row["Imposto Renda Normal"] = 0
        row["Imposto Renda Normal Final"] = -(row["Dedo Duro"])
        
    if row["Lucro Bruto Compensado"] > 0:
        row["Imposto Renda Compensado"] = row["Lucro Bruto Compensado"] * row["Taxa IR"]
        row["Imposto Renda Compensado Final"] = row["Imposto Renda Compensado"] - row["Dedo Duro"]
    else:
        row["Imposto Renda Compensado"] = 0
        row["Imposto Renda Compensado Final"] = -(row["Dedo Duro"])
    return row


def calcular_lucro_real(row):
    # Calcula o valor Real de Lucro descontando os prezuizos e impostos
    if row["Lucro/Prejuizo"] > 0:
        row["Lucro Real"] = row["Lucro Bruto"] - row["Imposto Renda Normal Final"]
    else:
        row["Lucro Real"] = 0
    return row


def isencao_imposto_renda(row):
    # Para vendas de ações com montante menor ou igual 
    # a 20.000,00 em um periodo de um 1 mes, será isento de declarar imposto sobre essas ações, 
    # sendo excessões, qualquer tipo de Day Trade ou qualquer outra categoria.
    isencao_imposto_renda = False
    if row["Tipo Estrategia"] == "Long Trade":
        if (row['Tipo Ativo'] == "Acao") & (row["Valor Venda"] <= 20000):
            isencao_imposto_renda = True
    row["Isencao Imposto Renda"] = isencao_imposto_renda
    return row