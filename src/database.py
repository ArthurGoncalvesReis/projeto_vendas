import sqlite3
import logging
from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

load_dotenv()

logger = logging.getLogger("DATABASE")

PROCESSED_PATH = os.getenv("DATA_PROCESSED_PATH")

class DatabaseConnector:

    def __init__(self, db_name = "empresa.db"):

        self.db_path = Path(PROCESSED_PATH) / db_name
        self.db_path.parent.mkdir(parents= True, exist_ok=True)
        logger.info("Conexão preparada para o banco")
        self.engine = create_engine(f"sqlite:///{self.db_path}")

    def _get_connection(self):
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao SQLite: {e}")
            return None
        
    def _create_schema(self, schema_path = "schema.sql"):
        path = Path(schema_path)

        if not path.exists():
            logger.error(f"Caminho para schema.sql não existe")
            return
        
        try:
            with self._get_connection() as conn:
                with open(path,'r',encoding='utf-8') as f:
                    sql_script = f.read()
                conn.executescript(sql_script)
                logger.info("Schema criado com sucesso")

        except sqlite3.Error as e:
            logger.exception(f"Erro ao criar schema: {e}")
        

    def insert_data(self, df: pd.DataFrame, table_name):
        
        if df.empty or df is None:
            logger.warning("Tentativa de insesir dados vazios no banco")
            return
        
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=1000
            )
            logger.info(f"Sucesso ao inserir {len(df)} linhas na tabela {table_name}.")

        except IntegrityError as e:
            logger.error(f"Erro de Integridade: Você está tentando inserir dados que já existem na tabela '{table_name}'.")
    

        except Exception as e:
            logger.exception(f"Erro ao inserir dados no banco: {e}")

    def load_data(self, query: str):
        try:
            df = pd.read_sql(query, self.engine)
            logger.info("Query realizada com sucesso.")
            return df

        except Exception as e:
            logger.error(f"Erro ao ler a query no banco: {e}")


        