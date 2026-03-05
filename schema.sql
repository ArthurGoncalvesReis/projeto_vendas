CREATE TABLE IF NOT EXISTS produtos(
    id_produto INTEGER PRIMARY KEY,
    nome_produto TEXT,
    preco_unitario REAL
);

CREATE TABLE IF NOT EXISTS clientes(
    id_cliente INTEGER PRIMARY KEY,
    regiao TEXT
);

CREATE TABLE IF NOT EXISTS vendas(
    id_venda INTEGER PRIMARY KEY,
    id_cliente INTEGER,
    id_produto INTEGER NOT NULL,
    quantidade INTEGER NOT NULL,
    data_venda DATE,

    FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente),
    FOREIGN KEY (id_produto) REFERENCES produtos(id_produto)
)