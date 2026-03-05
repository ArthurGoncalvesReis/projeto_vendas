import pandas as pd
import logging
from src.database import DatabaseConnector
import sys
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("BI DASHBOARD")

connector = DatabaseConnector()

def executive_vision_generator():

        query = """
            SELECT v.id_venda,
            p.nome_produto,
            p.preco_unitario,
            v.quantidade,
            v.quantidade * p.preco_unitario AS total_venda,
            c.regiao
            FROM vendas v
            LEFT INNER produtos p ON p.id_produto = v.id_produto
            LEFT INNER clientes c ON c.id_cliente = v.id_cliente
        """
        df = connector.load_data(query)

        if not (df.empty or df is None):
            return df

def price_revenue_correlation():
     
    query = """
            SELECT p.nome_produto,
            v.quantidade,
            p.preco_unitario
            FROM vendas v
            INNER JOIN produtos p ON p.id_produto = v.id_produto
    """
    df = connector.load_data(query)

    if df is None:
        logger.error("Falha na conexão com o banco. Abortando análise.")
        return

    if df.empty:
        logger.warning("A query não retornou dados. Verifique se o ETL foi executado.")
        print("Aviso: Nenhum dado de venda encontrado para gerar o gráfico.")
        return

    try:
        df['total_venda'] = df['quantidade'] * df['preco_unitario']
        df = df.groupby(['nome_produto','preco_unitario'])['total_venda'].sum().reset_index()
        df.columns = ['produto','preco','receita_total']
        
        plt.figure(figsize=(10,6))
        sns.set_theme(style='darkgrid')

        plot = sns.regplot(
            data=df,
            x='preco',
            y='receita_total'
        )

        plt.title("Análise de correlação: preço unitário vs receita total ")
        plt.xlabel("Preço unitário")
        plt.ylabel("Receita Total")
        

        plt.savefig("teste_regressao.png")
    except Exception as e:
         logger.critical(f"Erro ao gerar gráfico: {e}")

def seasonality(period = 'M'):
    
    formats = {
         'M': '%m/%Y',
         'W': 'W%U/%Y',
         'D': '%d/%m/%Y'

    }
    
    query = """
            SELECT 
                v.data_venda,
                v.quantidade,
                p.preco_unitario
            FROM vendas v
            INNER JOIN produtos p ON v.id_produto = p.id_produto

    """
    df = connector.load_data(query)

    try:
        df['total_venda'] = df['quantidade'] * df['preco_unitario']
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        df = df.groupby(df['data_venda'].dt.to_period(period))['total_venda'].sum().reset_index()
        df['data_venda'] = df['data_venda'].dt.strftime(formats[period])

        plt.figure(figsize=(12,8))
        sns.set_theme(style='darkgrid')
        
        plot = sns.lineplot(
            data=df,
            x='data_venda',
            y='total_venda',
            markers='o'
        )

        if(period == 'D'):
             plot.xaxis.set_major_locator(mdates.DayLocator(interval=7))

        plt.title("Sazonalidade das vendas")
        plt.xticks(rotation=45)
        plt.xlabel("Data")
        plt.ylabel("Receita R$")

        plt.savefig("teste_sazonalidade.png")

    except Exception as e:
         logger.critical(f"Erro ao gerar gráfico: {e}")

def revenue_by_region():
    query = """
        SELECT
            p.preco_unitario,
            v.quantidade,
            c.regiao
        FROM vendas v
        INNER JOIN produtos p ON p.id_produto = v.id_produto
        INNER JOIN clientes c ON c.id_cliente = v.id_cliente

    """
    df = connector.load_data(query)
    try:
        df['total_venda'] = df['quantidade'] * df['preco_unitario']
        df = df.groupby('regiao')['total_venda'].sum().reset_index()

        plot = plt.figure(figsize=(10,6))

        sns.barplot(
            data=df,
            x='regiao',
            y='total_venda'
        )

        plt.savefig('teste_regiao.png')

    
    except Exception as e:
        logger.critical(f"Erro ao gerar gráfico: {e}")
    print(df.head())

if __name__ == "__main__":
    #executive_vision_generator()
    #price_revenue_correlation()
    #seasonality()
    revenue_by_region()
    pass
