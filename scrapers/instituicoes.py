import pandas as pd
import ast
import re
from unidecode import unidecode

# Crição de dicionário de municípios de acordo com municipios_brasil.csv

df_municipios = pd.read_csv("./data/municipios_brasil.csv", encoding="utf-8")

# Normaliza o nome dos municípios e a sigla do estado
df_municipios["municipio_normalizado"] = df_municipios["municipio"].apply(
    lambda x: unidecode(str(x)).lower().strip() if pd.notnull(x) else ""
)
df_municipios["sigla_estado_normalizada"] = df_municipios["uf"].str.upper().str.strip()

# Cria o dicionário de cidades com nome do município normalizado e sigla do estado
cidades_estados = dict(zip(df_municipios["municipio_normalizado"], df_municipios["sigla_estado_normalizada"]))

# Processamento das instituições

"""
Processa uma string com os detalhes de uma instituição e extrai:
    - Nome da IES: token que contenha palavras-chave institucionais
    - Cidade e UF: Se houver um token com CEP, utiliza-o para extrair “cidade-estado”. Caso contrário, tenta obter esses dados a partir dos últimos tokens.
    - País: Utiliza o último token se composto apenas por letras.
    
Parâmetros:
    inst_str (str): String com detalhes da instituição, com itens separados por vírgula
    
Retorna:
    tupla: (instituição, cidade, UF, país) – strings
"""
def parse_institution_detail(inst_str):
    tokens = [t.strip() for t in inst_str.split(',') if t.strip()]
    
    institution = ""
    city = ""
    state = ""
    country = ""
    
    # Procura um token contendo CEP 
    location_token = None
    for token in tokens:
        if re.search(r'\d{5}-\d{3}', token):
            location_token = token
            break
    if location_token:
        cleaned = re.sub(r'\d{5}-\d{3}', '', location_token).strip()
        m = re.match(r'([a-zA-Z\s]+)-([a-zA-Z]+)', cleaned)
        if m:
            city = m.group(1).strip().lower()
            state = m.group(2).strip().upper()
    else:
        # Se não houver token com CEP, utiliza os últimos tokens
        rev_tokens = tokens[::-1]
        if rev_tokens:
            last = rev_tokens[0]
            if re.match(r'^[a-zA-Z\s]+$', last):
                country = last.lower()
            if len(rev_tokens) >= 2:
                possible_state = rev_tokens[1]
                if len(possible_state) == 2:
                    state = possible_state.upper()
                    if len(rev_tokens) >= 3:
                        city = rev_tokens[2].lower()
                else:
                    parts = possible_state.split('-')
                    if len(parts) == 2:
                        city = parts[0].strip().lower()
                        state = parts[1].strip().upper()
                    else:
                        city = possible_state.lower()
    
    if not country and tokens:
        last = tokens[-1]
        if re.match(r'^[a-zA-Z\s]+$', last):
            country = last.lower()
    
    # Extração do nome da instituição
    include_keywords = ["universidade", "instituto", "faculdade", "escola", "centro"]
    exclude_keywords = ["departamento", "curso", "laboratório", "programa"]
    inst_candidates = []
    for token in tokens:
        token_lower = token.lower()
        if any(kw in token_lower for kw in include_keywords) and not any(ex_kw in token_lower for ex_kw in exclude_keywords):
            inst_candidates.append(token)
    if inst_candidates:
        institution = max(inst_candidates, key=len).lower()
        institution = re.sub(r'\s*\([^)]*\)', '', institution).strip()
    else:
        for token in tokens:
            token_lower = token.lower()
            if any(kw in token_lower for kw in include_keywords):
                inst_candidates.append(token)
        if inst_candidates:
            institution = max(inst_candidates, key=len).lower()
            institution = re.sub(r'\s*\([^)]*\)', '', institution).strip()
    
    return institution, city, state, country

"""
Processa a coluna "institutions" de uma publicação. A string de entrada é avaliada para obter uma lista de instituições. Para cada instituição, extrai o nome da IES, cidade, UF e país usando parse_institution_detail(). Apenas instituições cujo país seja "brazil" são consideradas. Os resultados de cada campo são concatenados por ";"

Parâmetros:
    inst_str (str): String representando uma lista de instituições
    
Retorna:
    dict: Com chaves "Instituicao", "Cidade", "Estado" e "Pais"
"""
def process_institutions_column(inst_str):
    try:
        institutions = ast.literal_eval(inst_str)
    except Exception:
        return {"Instituicao": "", "Cidade": "", "Estado": "", "Pais": ""}
    
    names = []
    cities = []
    states = []
    countries = []
    
    for inst in institutions:
        nome, cidade, uf, pais = parse_institution_detail(inst)
        if pais != "brazil":
            continue
        if nome:
            names.append(nome)
        if cidade:
            cities.append(cidade)
        if uf:
            states.append(uf)
        if pais:
            countries.append(pais)
    
    return {
        "Instituicao": "; ".join(names),
        "Cidade": "; ".join(cities),
        "Estado": "; ".join(states),
        "Pais": "; ".join(countries)
    }

"""
Para uma string de cidades separadas por ";", utiliza exclusivamente o dicionário de cidades (baseado no arquivo de municípios) para retornar a sigla do estado de cada cidade

Parâmetros:
    city_str (str): Cidades separadas por ";"
    
Retorna:
    str: Siglas dos estados correspondentes, separadas por ";"
"""
def map_state_from_city(city_str):
    if not city_str:
        return ""
    
    cities = [c.strip() for c in city_str.split(";") if c.strip()]
    states = []
    for city in cities:
        city_norm = unidecode(city).lower().strip()
        state = cidades_estados.get(city_norm, "")
        if state:
            states.append(state)
        else:
            states.append("")
    # Remove entradas vazias e junta com ";"
    states = [s for s in states if s]
    return "; ".join(states)
