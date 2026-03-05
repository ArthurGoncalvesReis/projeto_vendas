import pandas as pd
import logging
from src.database import DatabaseConnector
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("BI DASHBOARD")

connector = DatabaseConnector()

def executive_vision_generator():

        query = """
            SELECT p.nome_produto,
            p.preco_unitario,
            v.quantidade,
            v.quantidade * p.preco_unitario AS total_venda,
            c.regiao
            FROM vendas v
            LEFT JOIN produtos p ON p.id_produto = v.id_produto
            LEFT JOIN clientes c ON c.id_cliente = v.id_cliente
        """
        df = connector.load_data(query)

        if not (df.empty or df is None):

            relatorio_regiao = df.groupby('regiao')['total_venda'].sum()
            ticket_medio = df.groupby('nome_produto')['total_venda'].mean().sort_values(ascending=False)
            print("=========Faturamento por região=========")
            print(relatorio_regiao)
            print("==============Ticket médio==============")
            print(ticket_medio)
        

if __name__ == "__main__":
    logger.debug("S")
    executive_vision_generator()
