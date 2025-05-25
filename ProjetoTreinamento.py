import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime
import locale
from sqlalchemy import create_engine
from urllib.parse import quote

locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')

# Importando arquivos
caminho = r"C:\Users\marco\Downloads\vendas_diarias"
arquivos = glob.glob(os.path.join(caminho, "*.csv"))
dados = pd.concat([pd.read_csv(arquivo, encoding="utf-8")
                  for arquivo in arquivos], ignore_index=True)

# Processamento de datas
dados['data'] = pd.to_datetime(dados['data'])
dados['dia'] = dados['data'].dt.day
dados['mes'] = dados['data'].dt.strftime('%b')
dados['Ano'] = dados['data'].dt.year

# Inicializando colunas com 0
cols_para_zero = ['Vendas_Paga', 'Lucro', 'Lucro_percent', 'Prejuizo', 'Pendente', 'Custo_Pago', 'Custo_Cancelado', 'Custo_Pendente',
                  'Vendas_Canceladas', 'Vendas_Pendente']
dados[cols_para_zero] = 0.0

# Mapeamento de status
status_map = {
    'Pago': {
        'Vendas_Paga': 'valor_venda',
        'Custo_Pago': 'custo',
        'Lucro': lambda x: x['valor_venda'] - x['custo'],
        'Lucro_percent': lambda x: (x['valor_venda'] - x['custo']) / x['valor_venda']
    },
    'Cancelado': {
        'Prejuizo': 'custo',
        'Vendas_Canceladas': 'valor_venda',
        'Custo_Cancelado': 'custo'
    },
    'Pendente': {
        'Pendente': 'valor_venda',
        'Vendas_Pendente': 'valor_venda',
        'Custo_Pendente': 'custo'
    }
}

# Cálculos
for status, col_map in status_map.items():
    mask = dados['status_pagamento'] == status
    for col, source in col_map.items():
        if callable(source):
            dados.loc[mask, col] = source(dados.loc[mask])
        else:
            dados.loc[mask, col] = dados.loc[mask, source]

# DataFrame consolidados
consolidado_map = {
    'Custo_Total': 'custo',
    'Lucro_Total': 'Lucro',
    'Prejuizo_Total': 'Prejuizo',
    'Total_Pendente': 'Pendente',
    'Total_Custo_Pago': 'Custo_Pago',
    'Total_Custo_Cancelado': 'Custo_Cancelado',
    'Total_Custo_Pendente': 'Custo_Pendente',
    'Total_Vendas_Pagas': 'Vendas_Paga',
    'Total_Vendas_Canceladas': 'Vendas_Canceladas',
    'Total_Vendas_Pendentes': 'Vendas_Pendente'
}

# Criando um dicionário com todos os valores
consolidado_data = {
    'data_referencia': [pd.Timestamp.now().strftime('%Y-%m-%d')],
    'tipo_registro': ['consolidado_diario']
}
# Adicionando cada métrica ao dicionário
for col_name, source_col in consolidado_map.items():
    consolidado_data[col_name.lower()] = [dados[source_col].sum()]

# Criando o DataFrame
df_consolidados = pd.DataFrame(consolidado_data)

# Função para enviar dados ao banco


def enviar_para_banco(df_consolidados, dados):
    usuario = 'postgres'
    senha = '*****'
    host = '
    porta = '5432'
    banco = ''

    senha_encoded = quote(senha)
    conn = create_engine(
        f'postgresql+psycopg2://{usuario}:{senha_encoded}@{host}:{porta}/{banco}')

    # Cópias para evitar modificar os DataFrames originais
    df_consolidados_envio = df_consolidados.copy()
    dados_envio = dados.copy()

    # Padroniza nomes de colunas para minúsculas
    df_consolidados_envio.columns = [col.lower()
                                     for col in df_consolidados_envio.columns]
    dados_envio.columns = [col.lower() for col in dados_envio.columns]

    # >>>>> VENDAS DETALHADAS <<<<<
    try:
        with conn.connect() as con:
            existentes_detalhadas = pd.read_sql(
                "SELECT data, id_cliente, produto FROM vendas_detalhadas", con)
            dados_envio['data'] = pd.to_datetime(dados_envio['data']).dt.date
            existentes_detalhadas['data'] = pd.to_datetime(
                existentes_detalhadas['data']).dt.date

            novos_detalhados = dados_envio.merge(
                existentes_detalhadas,
                on=['data', 'id_cliente', 'produto'],
                how='left',
                indicator=True
            ).query('_merge == "left_only"').drop(columns=['_merge'])

            if not novos_detalhados.empty:
                novos_detalhados.to_sql(
                    'vendas_detalhadas', con=conn, if_exists='append', index=False)
                print("Novos dados de vendas_detalhadas enviados com sucesso.")
            else:
                print("Nenhum dado novo a enviar para vendas_detalhadas.")

    except Exception as e:
        print("Erro ao enviar dados detalhados:", e)

    # >>>>> VENDAS CONSOLIDADAS <<<<<
    try:
        with conn.connect() as con:
            # Verifica se já existe registro para esta data
            df_consolidados_envio['data_referencia'] = pd.to_datetime(
                df_consolidados_envio['data_referencia']).dt.date
            data_ref = df_consolidados_envio['data_referencia'].iloc[0]

            existe = pd.read_sql(
                f"SELECT 1 FROM vendas_consolidadas WHERE data_referencia = '{data_ref}' AND tipo_registro = 'consolidado_diario'",
                con
            )

            if existe.empty:
                df_consolidados_envio.to_sql(
                    'vendas_consolidadas', con=conn, if_exists='append', index=False)
                print("Dados consolidados enviados com sucesso.")
            else:
                print(
                    f"Já existe um registro consolidado para a data {data_ref}. Nada foi enviado.")

    except Exception as e:
        print("Erro ao enviar dados consolidados:", e)


# Execução principal
if __name__ == "__main__":
    enviar_para_banco(df_consolidados, dados)
