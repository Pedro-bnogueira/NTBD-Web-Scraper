import pandas as pd
from fuzzywuzzy import fuzz
import ast

# Carregar o DataFrame de publicações
df = pd.read_csv("articles_dw.csv", encoding="utf-8")

# Carregar o DataFrame do e‑MEC (base de instituições)
ref_df = pd.read_csv("emec_institutions.csv", encoding="utf-8")

def extrair_nome_instituicao(institutions_str):
    """
    Converte a string que representa uma lista de instituições e extrai o nome principal.
    Exemplo:
        "['departamento de quimica, universidade federal de santa catarina, 88040-900 florianopolis-sc, brazil']"
    Retorna:
        str: Nome principal da instituição (neste exemplo, 'universidade federal de santa catarina')
    """
    try:
        # Utiliza ast.literal_eval para segurança na conversão da string em lista
        institutions = ast.literal_eval(institutions_str)
    except Exception:
        return ""
    if institutions and isinstance(institutions, list):
        # Divide a primeira instituição por vírgulas e assume que o nome da IES é o segundo item
        parts = institutions[0].split(',')
        if len(parts) >= 2:
            return parts[1].strip().lower()
    return ""

def match_institution(nome_inst, ref_df, threshold=80):
    """
    Mapeia o nome da instituição com a base e‑MEC utilizando similaridade fuzzy.
    
    Args:
        nome_inst (str): Nome extraído da publicação.
        ref_df (DataFrame): DataFrame com a base e‑MEC (contendo as colunas "Nome da IES", "Município" e "UF").
        threshold (int): Similaridade mínima para considerar um match.
    
    Returns:
        dict: Dicionário com os dados mapeados (chaves: "Nome da IES", "Município", "UF") ou {} se nenhum match for encontrado.
    """
    best_score = 0
    best_match = None
    for idx, row in ref_df.iterrows():
        ies_name = str(row.get("Nome da IES", "")).lower()
        score = fuzz.token_set_ratio(nome_inst, ies_name)
        if score > best_score:
            best_score = score
            best_match = row
    if best_score >= threshold and best_match is not None:
        return {
            "Nome da IES": best_match.get("Nome da IES", ""),
            "Município": best_match.get("Município", ""),
            "UF": best_match.get("UF", "")
        }
    return {}

def process_institutions(institutions_str, ref_df, threshold=80):
    """
    Processa a string de instituições de uma publicação, extrai o nome principal
    e mapeia os dados da IES utilizando a base e‑MEC.
    
    Args:
        institutions_str (str): String representando uma lista de instituições.
        ref_df (DataFrame): DataFrame com os dados do e‑MEC (colunas "Nome da IES", "Município", "UF").
        threshold (int): Similaridade mínima para o match.
    
    Returns:
        dict: Dicionário com os dados mapeados (ou {} se não houver match).
    """
    nome_inst = extrair_nome_instituicao(institutions_str)
    if not nome_inst:
        return {}
    return match_institution(nome_inst, ref_df, threshold)

# Aplica o mapeamento em toda a coluna "institutions" do DataFrame
df["mapped_institution"] = df["institutions"].apply(lambda x: process_institutions(x, ref_df, threshold=80))

# Salva o DataFrame resultante em um novo CSV
df.to_csv("articles_dw_mapped.csv", index=False, encoding="utf-8")
print("Processamento completo. Dados salvos em articles_dw_mapped.csv")
