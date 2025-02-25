import psycopg2
import csv

def replace_empty_with_null(value):
    return None if value == '' else value

# Conectar ao banco de dados
conn = psycopg2.connect("dbname=DW_Projeto user=postgres password=SocorroDeus host=localhost")
cur = conn.cursor()

# Criar a tabela temporária
cur.execute("""
    CREATE TEMP TABLE Temp_Publicacoes (
        journal TEXT,
        year INT,
        volume TEXT,
        edition_number TEXT,
        publication_date DATE,
        publication_type TEXT,
        title TEXT,
        authors TEXT,
        keywords TEXT,
        TotalAccess FLOAT,
        subareas TEXT,
        year_extracted FLOAT,
        month_extracted FLOAT,
        day_extracted FLOAT,
        edition_id TEXT,
        instituicao TEXT,
        cidade TEXT,
        estado TEXT,
        pais TEXT
    );
""")

# Inserir dados na tabela temporária
with open("articles_dw.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # Pular cabeçalho
    for row in reader:
        row = [replace_empty_with_null(value) for value in row]
        cur.execute("""
            INSERT INTO Temp_Publicacoes (journal, year, volume, edition_number, publication_date, 
                publication_type, title, authors, keywords, TotalAccess, subareas, 
                year_extracted, month_extracted, day_extracted, edition_id, instituicao, cidade, estado, pais)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, row)

# Inserção nas tabelas dimensionais
cur.execute("""
    INSERT INTO Dim_Tempo (chaveTempo, decada, quinquenio, ano, mes)
    SELECT DISTINCT 
        publication_date AS chaveTempo,
        EXTRACT(YEAR FROM publication_date)::SMALLINT / 10 * 10 AS decada,
        EXTRACT(YEAR FROM publication_date)::SMALLINT / 5 * 5 AS quinquenio,
        EXTRACT(YEAR FROM publication_date)::SMALLINT AS ano,
        EXTRACT(MONTH FROM publication_date)::SMALLINT AS mes
    FROM Temp_Publicacoes
    WHERE publication_date IS NOT NULL
    ON CONFLICT (chaveTempo) DO NOTHING;
""")

cur.execute("""
    INSERT INTO Dim_TipoPublicacao (tipo_publicacao)
    SELECT DISTINCT publication_type
    FROM Temp_Publicacoes
    WHERE publication_type IS NOT NULL
    ON CONFLICT (tipo_publicacao) DO NOTHING;
""")

# Inserção na Dim_Autor separando os autores corretamente
cur.execute("""
    INSERT INTO Dim_Autor (nome_autor)
    SELECT DISTINCT TRIM(BOTH '"' FROM author_array.value) AS nome_autor
    FROM (
        SELECT string_to_array(
            regexp_replace(authors, E'[^\\\w\\\s,]', '', 'g'),  -- Remove '[' e ']' e aspas simples ao redor
            E', '  -- Usa ', ' como delimitador
        ) AS autores
        FROM Temp_Publicacoes
        WHERE authors IS NOT NULL
    ) t
    JOIN LATERAL unnest(t.autores) AS author_array(value) ON true
    ON CONFLICT (nome_autor) DO NOTHING;
""")

cur.execute("""
    INSERT INTO Dim_Instituicao (nome_instituicao, cidade, estado, pais)
    SELECT DISTINCT 
        TRIM(inst_array.value) AS nome_instituicao,
        TRIM(city_array.value) AS cidade,
        TRIM(state_array.value) AS estado,
        TRIM(country_array.value) AS pais
    FROM (
        SELECT 
            string_to_array(instituicao, ';') AS instituicoes,
            string_to_array(cidade, ';') AS cidades,
            string_to_array(estado, ';') AS estados,
            string_to_array(pais, ';') AS paises
        FROM Temp_Publicacoes
        WHERE instituicao IS NOT NULL
    ) t
    JOIN LATERAL unnest(t.instituicoes) WITH ORDINALITY AS inst_array(value, idx) ON true
    JOIN LATERAL unnest(t.cidades) WITH ORDINALITY AS city_array(value, idx) ON city_array.idx = inst_array.idx
    JOIN LATERAL unnest(t.estados) WITH ORDINALITY AS state_array(value, idx) ON state_array.idx = inst_array.idx
    JOIN LATERAL unnest(t.paises) WITH ORDINALITY AS country_array(value, idx) ON country_array.idx = inst_array.idx
    ON CONFLICT (nome_instituicao, cidade, estado, pais) DO NOTHING;
""")

# Inserção na Dim_PalavraChave separando palavras-chave corretamente
cur.execute("""
    INSERT INTO Dim_PalavraChave (palavraChave, subarea)
    SELECT DISTINCT 
        TRIM(BOTH '"' FROM keyword_array.value) AS palavraChave,
        TRIM(BOTH '"' FROM subarea_array.value) AS subarea
    FROM (
        SELECT 
            string_to_array(
                regexp_replace(keywords, E'[^\\\w\\\s,]', '', 'g'),  -- Remove '[' e ']' e aspas simples ao redor
                E', '  -- Usa ', ' como delimitador correto
            ) AS palavras_chave,
            string_to_array(
                regexp_replace(subareas, E'[^\\\w\\\s,]', '', 'g'),
                E', '
            ) AS subareas_list
        FROM Temp_Publicacoes
        WHERE keywords IS NOT NULL
    ) t
    JOIN LATERAL unnest(t.palavras_chave) WITH ORDINALITY AS keyword_array(value, idx) ON true
    JOIN LATERAL unnest(t.subareas_list) WITH ORDINALITY AS subarea_array(value, idx) ON subarea_array.idx = keyword_array.idx
    ON CONFLICT (palavraChave) DO NOTHING;
""")

# Inserção na Fato_Publicacao
cur.execute("""
    INSERT INTO Fato_Publicacao (titulo, chaveTempo, chaveTipoPublicacao, nome_revista, edicao, numero_acessos)
SELECT 
    p.title AS titulo,
    p.publication_date AS chaveTempo,
    tp.chaveTipoPublicacao AS chaveTipoPublicacao,
    p.journal AS nome_revista,
    p.edition_id AS edicao,
    p.TotalAccess AS numero_acessos
FROM Temp_Publicacoes p
JOIN Dim_TipoPublicacao tp ON p.publication_type = tp.tipo_publicacao
WHERE p.publication_date IS NOT NULL;
""")
cur.execute("""
INSERT INTO Ponte_Autor (chavePublicacao, chaveAutor)
SELECT 
    fp.titulo AS chavePublicacao,
    da.chaveAutor
FROM Fato_Publicacao fp
JOIN Temp_Publicacoes tp ON tp.title = fp.titulo
JOIN Dim_Autor da ON TRUE
JOIN LATERAL (
    SELECT unnest(
        string_to_array(
            replace(replace(replace(tp.authors, '[', ''), ']', ''), '''', ''), 
            ', '
        )
    ) AS autor
) autor_extraido ON TRIM(da.nome_autor) = TRIM(autor_extraido.autor)
ON CONFLICT DO NOTHING; -- Impede erro de duplicação

""")

cur.execute("""
INSERT INTO Ponte_Instituicao (chavePublicacao, chaveInstituicao)
SELECT 
    fp.titulo AS chavePublicacao,
    di.chaveInstituicao
FROM Fato_Publicacao fp
JOIN Temp_Publicacoes tp ON tp.title = fp.titulo
JOIN LATERAL unnest(
    string_to_array(tp.instituicao, ';')
) AS instituicao_extraida(nome_instituicao) ON TRUE
JOIN Dim_Instituicao di 
ON TRIM(di.nome_instituicao) = TRIM(instituicao_extraida.nome_instituicao)
ON CONFLICT DO NOTHING;

""")

cur.execute("""
INSERT INTO Ponte_PalavraChave (chavePublicacao, chavePalavraChave)
SELECT 
    fp.titulo AS chavePublicacao,
    dpk.chavePalavraChave
FROM Fato_Publicacao fp
JOIN Temp_Publicacoes tp ON tp.title = fp.titulo
JOIN LATERAL unnest(
    string_to_array(
        regexp_replace(tp.keywords, E'[^\\\w\\\s,]', '', 'g'), 
        E', '
    )
) AS palavra_extraida(palavraChave) ON TRUE
JOIN Dim_PalavraChave dpk 
ON TRIM(dpk.palavraChave) = TRIM(palavra_extraida.palavraChave)
ON CONFLICT DO NOTHING;

""")


conn.commit()  # Salvar todas as inserções
cur.close()
conn.close()