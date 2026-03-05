import pandas as pd
import numpy as np
from dotenv import load_dotenv
import logging
import sys
import os
from pathlib import Path

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("DATA GENERATOR")

def generate_raw_data():
    raw_path_env = os.getenv("DATA_RAW_PATH")
    
    base_path = Path(raw_path_env)
    base_path.mkdir(parents=True, exist_ok=True)

    try:
        vendas_data = {
            'id_venda': range(1, 101),
            'id_produto': np.random.randint(101, 106, size=100),
            'id_cliente': np.random.randint(1001, 1020, size=100),
            'quantidade': np.random.randint(1, 10, size=100),
            'data': pd.date_range(start='2026-01-01', periods=100, freq='D')
        }
        df_vendas = pd.DataFrame(vendas_data)
        df_vendas.loc[0:5, 'quantidade'] = np.nan
        
        file_path = base_path / 'vendas.csv'
        df_vendas.to_csv(file_path, index=False)
        
        logger.info(f"Dados de vendas criados em: {file_path}")
    except Exception as e:
        logger.error(f"Falha na criação dos dados de venda: {e}")

    try:
        produtos_data = {
            'id_produto': [101, 102, 103, 104, 105],
            'nome_produto': ['Widget A', 'Gadget B', 'Tool C', 'Device D', 'Model E'],
            'preco_unitario': [25.50, 40.00, 15.75, 120.00, 85.00]
        }
        file_path = base_path / 'produtos.csv'
        pd.DataFrame(produtos_data).to_csv(file_path, index=False)
        logger.info(f"Dados dos produtos criados em: {file_path}")
    except Exception as e:
        logger.error(f"Erro na criação dos dados dos produtos: {e}")

    try:
        clientes_data = {
            'id_cliente': range(1001, 1021),
            'regiao': np.random.choice(['Norte', 'Sul', 'Leste', 'Oeste'], size=20)
        }
        file_path = base_path / 'clientes.csv'
        pd.DataFrame(clientes_data).to_csv(file_path, index=False)
        logger.info(f"Dados dos clientes criados em: {file_path}")
    except Exception as e:
        logger.error(f"Erro na criação dos dados dos clientes: {e}")

if __name__ == "__main__":
    generate_raw_data()