import pandas as pd
import numpy as np
from dotenv import load_dotenv
import logging
import sys
import os
from pathlib import Path

load_dotenv()

logger = logging.getLogger("ETL")

def clean_vendas(df_vendas: pd.DataFrame):
    try:
        df_vendas['quantidade'] = df_vendas['quantidade'].fillna(df_vendas['quantidade'].median())
        return df_vendas
        
    except Exception as e:
        logger.error(f"Erro ao exibir df: {e}")

def clean_clientes(df_clientes: pd.DataFrame):
    df_clientes['id_cliente'] = df_clientes['id_cliente'].dropna()
    df_clientes['regiao'] = df_clientes['regiao'].fillna(df_clientes['regiao'].mode())
    return df_clientes

def clean_produtos(df_produtos: pd.DataFrame):
    df_produtos['id_produto'] = df_produtos['id_produto'].dropna()
    df_produtos = df_produtos[~(df_produtos['nome_produto'].isna() & df_produtos['preco_unitario'].isna())]
    return df_produtos

def transform_vendas(df_vendas: pd.DataFrame):
    df_vendas['id_venda'] = df_vendas['id_venda'].astype(np.int32)
    df_vendas['id_cliente'] = df_vendas['id_cliente'].astype(np.int32)
    df_vendas['id_produto'] = df_vendas['id_produto'].astype(np.int32)
    df_vendas['quantidade'] = df_vendas['quantidade'].astype(np.int32)
    df_vendas['data'] = df_vendas['data'].astype('datetime64[ns]')
    df_vendas = df_vendas.rename(columns={'data':'data_venda'})
    return df_vendas

def transform_clientes(df_clientes: pd.DataFrame):
    df_clientes['id_cliente'] = df_clientes['id_cliente'].astype(np.int32)
    df_clientes['regiao'] = df_clientes['regiao'].astype('category')
    return df_clientes

def transform_produtos(df_produtos: pd.DataFrame):
    df_produtos['id_produto'] = df_produtos['id_produto'].astype(np.int32)
    df_produtos['nome_produto'] = df_produtos['nome_produto'].astype('category')
    df_produtos['preco_unitario'] = df_produtos['preco_unitario'].astype(np.float64)
    return df_produtos
