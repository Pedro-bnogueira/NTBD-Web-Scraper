import pandas as pd
import re
import unidecode
from rapidfuzz import fuzz


"""
Padroniza os nomes das instituições a partir de uma string separada por ponto e vírgula

Parâmetros:
    instituicoes_str (str): String com os nomes das instituições separados por ';'

Retorna:
    list: Lista de instituições padronizadas (sem acentos, em minúsculas e sem duplicatas)
"""
def padronizar_instituicoes(instituicoes_str):
    if not instituicoes_str:
        return []
    
    # Divide a string pelos ponto e vírgula e remove espaços extras
    instituicoes = [inst.strip() for inst in instituicoes_str.split(';') if inst.strip()]
    # Remove acentos e converte para minúsculas
    instituicoes = [unidecode.unidecode(inst).lower() for inst in instituicoes]
    # Remove duplicatas preservando a ordem
    vistas = []
    for inst in instituicoes:
        if inst not in vistas:
            vistas.append(inst)
    return vistas

"""
Normaliza uma string de palavras-chave separadas por ponto e vírgula

Parâmetros:
    keywords_str (str): String contendo as palavras-chave separadas por ';'

Retorna:
    list: Lista de palavras-chave normalizadas (sem acentos e em minúsculas)
"""
def normalizar_palavras_chave(keywords_str):
    if not keywords_str:
        return []
    # Divide a string pelo separador ';' e remove espaços extras
    keywords = [kw.strip() for kw in keywords_str.split(';') if kw.strip()]
    # Remove acentos e converte para minúsculas (utilize a biblioteca unidecode, se disponível)
    keywords = [unidecode.unidecode(kw).lower() for kw in keywords]
    return keywords

"""
Mapeia uma lista de palavras-chave para as subáreas da química, utilizando similaridade sintática, utilizando três métricas: fuzz.ratio, fuzz.token_set_ratio e fuzz.partial_ratio. Se o maior dos scores for maior ou igual ao threshold, a subárea é adicionada ao conjunto de subáreas identificadas.

Parâmetros:
    keywords (list): Lista de palavras-chave 
    subarea_map (dict): Dicionário onde as chaves são nomes das subáreas e os valores são listas de termos representativos dessa subárea
    threshold (int): Pontuação mínima de similaridade para considerar um match (padrão: 75)

Retorna:
    list: Lista de subáreas identificadas com base nas palavras-chave
"""
def mapear_subareas(keywords, subarea_map, threshold=75):
    subareas_encontradas = set()
    
    for kw in keywords:
        # Normaliza a palavra-chave
        kw_norm = kw.lower().strip()
        for subarea, termos in subarea_map.items():
            for termo in termos:
                # Normaliza o termo do dicionário
                termo_norm = termo.lower().strip()
                score1 = fuzz.ratio(kw_norm, termo_norm)
                score2 = fuzz.token_set_ratio(kw_norm, termo_norm)
                score3 = fuzz.partial_ratio(kw_norm, termo_norm)
                max_score = max(score1, score2, score3)
                if max_score >= threshold:
                    subareas_encontradas.add(subarea)
                    break  # Se encontrar um match para essa subárea, passa para a próxima palavra-chave
    return list(subareas_encontradas)

# Dicionário para mapeamento de subáreas
subarea_map = {
    "Química Orgânica": [
        "organic synthesis", "heterocyclic", "organometallic", "organic reaction",
        "quinones", "naphthoquinones", "aldol", "condensation", "nucleophilic substitution",
        "click chemistry", "esterification", "rearrangement", "C–C bond formation",
        "functional group transformation", "pericyclic reaction", "polymerization",
        "asymmetric synthesis", "radical reaction", "synthesis strategy"
    ],
    "Química Analítica": [
        "spectroscopy", "chromatography", "hplc", "gc-ms", "icp-ms", "mass spectrometry",
        "fluorescence", "uv-vis", "ftir", "nmr", "chemiluminescence", "colorimetric",
        "quantification", "analysis", "detection", "electroanalytical", "titrimetry",
        "biosensing", "sensor", "signal processing"
    ],
    "Química Inorgânica": [
        "inorganic", "coordination", "metal complex", "catalysis", "lanthanides",
        "metal oxide", "nanoparticle", "zeolite", "hydrotalcite", "perovskite",
        "spin crossover", "complexation", "ionic", "redox", "solid state",
        "crystal structure", "inorganic synthesis", "electronic configuration"
    ],
    "Físico-Química": [
        "kinetics", "thermodynamics", "electrochemistry", "surface", "dft", "density functional",
        "reaction mechanism", "rate constant", "activation energy", "electronic structure",
        "spectral analysis", "quantum", "coupled cluster", "vibrational", "microkinetic",
        "molecular dynamics", "phase transition", "statistical mechanics"
    ],
    "Bioquímica": [
        "biochemical", "enzyme", "metabolism", "protein", "antioxidant", "bioavailability",
        "lipid", "dna", "rna", "antimicrobial", "cellular", "hormone", "biosynthesis",
        "metabolic", "immuno", "signal transduction", "biomarker", "molecular biology"
    ],
    "Nanotecnologia e Materiais": [
        "nanomaterials", "nanotube", "graphene", "nanocomposite", "nanoflower",
        "nanotechnology", "carbon nanotube", "mesoporous", "mof", "film", "electrode",
        "polymer", "composite", "nanofabrication", "nanostructure", "nanowire",
        "self-assembly", "nanopatterning", "surface modification"
    ],
    "Environmental Chemistry": [
        "environmental", "contaminant", "pollutant", "wastewater", "adsorption",
        "bioaccumulation", "biomonitor", "soil", "water", "air quality", "trace element",
        "remediation", "sustainability", "eco-friendly", "environmental impact",
        "toxicology", "waste management", "green chemistry", "climate change"
    ],
    "Computational Chemistry": [
        "computational", "molecular docking", "simulation", "modeling", "qsar",
        "machine learning", "virtual screening", "quantum", "multivariate", "in silico",
        "data fusion", "computational chemistry", "predictive modeling", "bioinformatics",
        "cheminformatics", "molecular dynamics", "statistical modeling"
    ],
    "Química Educacional": [
        "education", "teaching", "curriculum", "pedagogy", "didactic", "instruction",
        "chemistry education", "university", "college", "learning", "training",
        "science communication", "educational methodology", "curricular development"
    ],
    "Industrial Chemistry": [
        "industrial", "process", "manufacturing", "scale-up", "optimization", "quality control",
        "production", "catalyst", "reaction engineering", "process optimization",
        "industrial", "technology", "automation", "sustainability in industry", "cost reduction"
    ],
    "Medicinal Chemistry": [
        "drug design", "pharmaceutical", "medicinal", "lead optimization", "synthesis",
        "structure-activity", "bioactive", "cheminformatics", "molecular docking",
        "pharmacology", "in vitro", "in vivo", "toxicity", "SAR", "ADMET"
    ],
    "Materials Science": [
        "materials", "composite", "mechanical", "thermal", "electrical", "magnetic",
        "nanostructure", "semiconductor", "ceramic", "polymer", "biomaterial",
        "fabrication", "characterization", "microstructure", "performance",
        "structural analysis"
    ],
    "Green Chemistry": [
        "green chemistry", "sustainable", "eco-friendly", "renewable", "biodegradable",
        "waste valorization", "circular economy", "solvent-free", "energy efficient",
        "low toxicity", "environmentally benign", "life cycle", "carbon footprint"
    ]
}

"""
Separa e padroniza os nomes dos autores a partir de uma string separada por ponto e vírgula

Parâmetros:
    autores_str (str): String com os nomes dos autores separados por ';'
                        Se não for uma string, retorna uma lista vazia

Retorna:
    list: Lista de nomes de autores padronizados (sem acentos e espaços extras), sem duplicatas
"""
def separar_autores(autores_str):
    if not isinstance(autores_str, str):
        return []
    autores = [a.strip() for a in autores_str.split(';') if a.strip()]
    autores = [unidecode.unidecode(a).strip() for a in autores]
    vistos = []
    for a in autores:
        if a not in vistos:
            vistos.append(a)
    return vistos


"""
Converte uma data para o formato 'YYYY-MM-DD' e extrai ano, mês e dia

Parâmetros:
    data_str (str): Data em formato variado

Retorna:
    dict: Dicionário com as chaves 'full_date', 'year', 'month' e 'day';
    Retorna None se a conversão falhar
"""
def converter_extrair_datas(data_str):
    try:
        dt = pd.to_datetime(data_str, errors='coerce')
        if pd.isnull(dt):
            return None
        return {
            "full_date": dt.strftime("%Y-%m-%d"),
            "year": dt.year,
            "month": dt.month,
            "day": dt.day
        }
    except Exception:
        return None

"""
Cria um identificador único para a edição, concatenando volume e número

Parâmetros:
    volume (str ou int): O volume da edição
    edition_number (str): O número da edição

Retorna:
    str: Identificador no formato "Vol.<volume>, No.<edition_number>"
"""
def concatenar_volume_numero(volume, edition_number):
    return f"Vol.{volume}, No.{edition_number}"


"""
Converte o título para letras minúsculas e remove espaços extras

Parâmetros:
    titulo (str): Título do artigo

Retorna:
    str: Título normalizado. 
    Se o título não for uma string, retorna uma string vazia
"""
def normalizar_titulo(titulo):
    if not isinstance(titulo, str):
        return ""
    titulo = titulo.lower().strip()
    titulo = re.sub(r'\s+', ' ', titulo)
    return titulo

"""
Remove registros duplicados, mantendo apenas o primeiro artigo com o mesmo título (após normalização)

Parâmetros:
    df (pd.DataFrame): DataFrame contendo os dados dos artigos, incluindo a coluna 'title'

Retorna:
    pd.DataFrame: DataFrame sem registros duplicados baseados no título normalizado
"""
def remover_duplicatas_revistas(df):
    df['title_norm'] = df['title'].apply(lambda t: normalizar_titulo(t))
    df_unique = df.drop_duplicates(subset=['title_norm'])
    df_unique = df_unique.drop(columns=['title_norm'])
    return df_unique

"""
Lê os dados do CSV de entrada, aplica operações de limpeza e transformação, e salva o resultado em um novo CSV

Operações:
    - Padroniza os nomes das instituições
    - Normaliza as palavras-chave
    - Mapeia as palavras-chave para subáreas da química e adiciona a coluna "subareas"
    - Separa e padroniza os nomes dos autores
    - Converte datas para o formato consistente e extrai componentes (ano, mês, dia)
    - Cria um identificador único para cada edição (concatenando volume e número)
    - Remove registros duplicados (mesmo título) entre revistas

Parâmetros:
    input_csv (str): Caminho para o CSV de entrada
    output_csv (str): Caminho para o CSV final processado

Retorna:
    None
"""
def tratar_dados(input_csv, output_csv):
    df = pd.read_csv(input_csv)

    # Padronizar instituições
    df['institutions'] = df['institutions'].apply(lambda x: padronizar_instituicoes(x) if isinstance(x, str) else [])
    
    # Normalizar palavras-chave
    df['keywords'] = df['keywords'].apply(lambda x: normalizar_palavras_chave(x) if isinstance(x, str) else [])

    # Mapeamento para subáreas: para cada registro, mapeia as palavras-chave para uma lista de subáreas.
    df['subareas'] = df['keywords'].apply(lambda kws: mapear_subareas(kws, subarea_map) if kws else [])
    
    # Separar e padronizar autores
    df['authors'] = df['authors'].apply(separar_autores)
    
    # Converter e extrair dados da data
    df['date_info'] = df['publication_date'].apply(converter_extrair_datas)
    df['publication_date'] = df['date_info'].apply(lambda d: d["full_date"] if d else "")
    df['year_extracted'] = df['date_info'].apply(lambda d: d["year"] if d else None)
    df['month_extracted'] = df['date_info'].apply(lambda d: d["month"] if d else None)
    df['day_extracted'] = df['date_info'].apply(lambda d: d["day"] if d else None)
    df.drop(columns=['date_info'], inplace=True)
    
    # Criar identificador único para a edição
    df['edition_id'] = df.apply(lambda row: concatenar_volume_numero(row['volume'], row['edition_number']), axis=1)
    
    # Remover duplicatas entre revistas (pelo título)
    df = remover_duplicatas_revistas(df)
    
    # Salvar o DataFrame processado
    df.to_csv(output_csv, index=False, encoding='utf-8')
    print(f"Dados tratados e salvos em {output_csv}")