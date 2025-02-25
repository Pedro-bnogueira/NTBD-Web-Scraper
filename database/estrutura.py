import os
import psycopg2
from psycopg2 import sql

# Função para conectar ao banco de dados
def connect_to_db():
    try:
        # Conectar ao banco de dados PostgreSQL
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME_PADRAO"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
        )
        conn.autocommit = True  # Para permitir criação do banco
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Criar o banco de dados, caso não exista
def create_database(conn):
    try:
        cur = conn.cursor()
        # Criar banco de dados 'DW_Projeto', caso não exista
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'DW_Projeto'")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute('CREATE DATABASE "DW_Projeto"')
            print("Banco de dados 'DW_Projeto' criado com sucesso.")
        else:
            print("Banco de dados 'DW_Projeto' já existe.")
        
        cur.close()
    except Exception as e:
        print(f"Erro ao criar banco de dados: {e}")

# Função para rodar o script SQL para criação das tabelas
def run_sql_script(conn, script):
    try:
        cur = conn.cursor()
        cur.execute(script)  # Executar o script SQL
        conn.commit()  # Confirmar as mudanças
        print("Estrutura de tabelas criada com sucesso!")
        cur.close()
    except Exception as e:
        print(f"Erro ao executar o script SQL: {e}")

# SQL script de criação das tabelas
sql_script = """
-- Dropar tabelas, se necessário
DROP TABLE IF EXISTS Fato_Publicacao, Ponte_PalavraChave, Dim_PalavraChave, Ponte_Instituicao, Dim_Instituicao, Ponte_Autor, Dim_Autor, Dim_TipoPublicacao, Dim_Tempo CASCADE;

-- Tabelas dimensionais
CREATE TABLE Dim_Tempo (
    chaveTempo DATE NOT NULL PRIMARY KEY, 
    decada SMALLINT NOT NULL, 
    quinquenio SMALLINT NOT NULL, 
    ano SMALLINT NOT NULL, 
    mes SMALLINT NOT NULL
);

CREATE TABLE Dim_TipoPublicacao (
    chaveTipoPublicacao SERIAL PRIMARY KEY,
    tipo_publicacao TEXT NOT NULL UNIQUE
);

CREATE TABLE Dim_Autor (
    chaveAutor SERIAL PRIMARY KEY,
    nome_autor TEXT NOT NULL UNIQUE
);

CREATE TABLE Dim_Instituicao (
    chaveInstituicao SERIAL PRIMARY KEY,
    nome_instituicao TEXT NOT NULL,  
    cidade TEXT, 
    estado TEXT,
    regiao TEXT, 
    pais TEXT,
    CONSTRAINT unique_nome_instituicao UNIQUE (nome_instituicao, cidade, estado, pais)
);

CREATE TABLE Dim_PalavraChave (
    chavePalavraChave SERIAL PRIMARY KEY,  
    palavraChave TEXT NOT NULL UNIQUE,  
    subarea TEXT  
);

-- Tabela fato, contém as medidas quantitativas e as chaves para as tabelas dimensionais
CREATE TABLE Fato_Publicacao (
    titulo text PRIMARY KEY,
    chaveTempo DATE NOT NULL, 
    chaveTipoPublicacao INT NOT NULL,
    nome_revista TEXT NOT NULL,
    edicao TEXT NOT NULL,
    numero_acessos INT NOT NULL, 

    FOREIGN KEY (chaveTempo) REFERENCES Dim_Tempo(chaveTempo),
    FOREIGN KEY (chaveTipoPublicacao) REFERENCES Dim_TipoPublicacao(chaveTipoPublicacao)
);
CREATE TABLE Ponte_Autor (
    chavePublicacao text NOT NULL,
    chaveAutor INT NOT NULL,
	PRIMARY KEY (chavePublicacao, chaveAutor),
    FOREIGN KEY (chavePublicacao) REFERENCES Fato_Publicacao(titulo) ON DELETE CASCADE,
    FOREIGN KEY (chaveAutor) REFERENCES Dim_Autor(chaveAutor)
);

CREATE TABLE Ponte_Instituicao ( 
    chavePublicacao text NOT NULL,
    chaveInstituicao INT NOT NULL,
	primary key (chavePublicacao, chaveInstituicao),
    FOREIGN KEY (chavePublicacao) REFERENCES Fato_Publicacao(titulo) ON DELETE CASCADE,
    FOREIGN KEY (chaveInstituicao) REFERENCES Dim_Instituicao(chaveInstituicao)
);

CREATE TABLE Ponte_PalavraChave ( 
	chavePublicacao text NOT NULL,
    chavePalavraChave INT NOT NULL,
	primary key(chavePublicacao, chavePalavraChave),
    FOREIGN KEY (chavePublicacao) REFERENCES Fato_Publicacao(titulo) ON DELETE CASCADE,
    FOREIGN KEY (chavePalavraChave) REFERENCES Dim_PalavraChave(chavePalavraChave)
);
"""