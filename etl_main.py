import pandas as pd
import numpy as np
from dotenv import load_dotenv
import logging
import sys
import os
from pathlib import Path
from src.etl import *
import re
from src.database import DatabaseConnector

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("MAIN")

PATH_RAW = os.getenv("DATA_RAW_PATH")
PATH_RAW = Path(PATH_RAW)
PATH_PROCESSED = os.getenv("DATA_PROCESSED_PATH")

connector = DatabaseConnector()

def run_pipeline():

    logger.debug(f"Entrando no método run_pipeline.")
    if not PATH_RAW.exists():
        raise FileNotFoundError("Pasta raw não encontrada.")
    
    try:
        for file in PATH_RAW.iterdir():
            if re.search(r'vendas',file.name, re.IGNORECASE):
                df_vendas_clean = clean_vendas(pd.read_csv(file))
                logger.info(f"Lendo arquivo de vendas {file.name}")
                df_vendas_tipadas = transform_vendas(df_vendas_clean)
                connector.insert_data(df_vendas_tipadas, "vendas")
            
            elif re.search(r'clientes',file.name, re.IGNORECASE):
                df_clientes_clean = clean_clientes(pd.read_csv(file))
                logger.info(f"Lendo arquivo de clientes {file.name}")
                df_clientes_tipados = transform_clientes(df_clientes_clean)
                connector.insert_data(df_clientes_tipados, "clientes")

            elif re.search(r'produtos',file.name, re.IGNORECASE):
                logger.info(f"Lendo arquivo de clientes {file.name}")
                df_produtos_clean = clean_produtos(pd.read_csv(file))
                df_produtos_tipados = transform_produtos(df_produtos_clean)
                connector.insert_data(df_produtos_tipados, "produtos")
                
            
    except Exception:
        logger.exception(f"Erro ao tratar o arquivo {file.name}")

    
if __name__ == "__main__":
    connector._create_schema()
    run_pipeline()
    connector.load_data("SELECT count(*) AS total FROM vendas")

