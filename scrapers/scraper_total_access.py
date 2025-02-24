"""
Funções para fazer o scraping do "Total Access" nos site SBQ da Química Nova e JBCS  e associar esses dados aos registros do arquivo 'articles.csv', fazendo a correspondência pelo título
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import time

"""
Converte o título para letras minúsculas e remove espaços extras

Parâmetros:
    titulo (str): Título do artigo

Retorna:
    str: Título normalizado
"""
def normalizar_titulo(titulo):
    titulo = titulo.lower().strip()
    titulo = re.sub(r'\s+', ' ', titulo)
    return titulo

"""
Realiza o scraping da página de edições anteriores, da Química Nova no portal SBQ
Cada chave do dicionário é uma tupla contendo (ano, volume) (ambos inteiros) e o valor é uma lista de tuplas,onde cada tupla representa uma edição e contém:
      - O número da edição (str) (por exemplo, "1", "2", "10Sup", etc)
      - A URL para acessar essa edição

    Retorna:
        dict: { (ano: int, volume: int): [(numero_edição: str, url_edição: str), ...] }
        Se a tabela não for encontrada ou ocorrer algum erro, retorna um dicionário vazio
    
"""
def get_edicoes_anteriores_qn():
    print("[QN] Iniciando o scraping das edições anteriores (Química Nova)...")
    base_url = 'https://quimicanova.sbq.org.br/'
    url = base_url + 'edicoes_anteriores.asp'
    try:
        r = requests.get(url)
        r.raise_for_status()
    except Exception as e:
        print(f"[QN] Erro ao acessar {url}: {e}")
        return {}
    
    soup = BeautifulSoup(r.text, 'html.parser')
    edicoes_dict = {}
    tabela = soup.find('table', {'border': '0', 'align': 'center'})
    if not tabela:
        print("[QN] Tabela principal não encontrada!")
        return edicoes_dict
    
    trs = tabela.find_all('tr')
    for tr in trs:
        tds = tr.find_all('td')
        if len(tds) < 2:
            continue
        try:
            ano_str = tds[0].get_text(strip=True)
            vol_str = tds[1].get_text(strip=True)
            ano = int(ano_str)
            vol = int(vol_str)
        except:
            continue
        edicoes_list = []
        for td_edicao in tds[2:]:
            link = td_edicao.find('a')
            if link:
                href = link.get('href', '')
                numero = link.text.strip().split('\n')[0]  # "1", "2", "10Sup", etc.
                edicoes_list.append((numero, base_url + href))
        edicoes_dict[(ano, vol)] = edicoes_list
    return edicoes_dict

"""
Extrai os artigos de uma edição da Química Nova a partir da URL fornecida (SBQ)
Retorna um dicionário onde as chaves são os títulos normalizados e os valores são o valor de "Total access" (ou None se não encontrado).

Parâmetros:
    url_edicao (str): URL completa da edição

Retorna:
    dict: { titulo_normalizado (str): total_access (int ou None) }
"""
def get_artigos_de_uma_edicao_qn(url_edicao):
    print(f"[QN] Buscando artigos em {url_edicao}")
    artigos = {}
    try:
        r = requests.get(url_edicao)
        r.raise_for_status()
    except Exception as e:
        print(f"[QN] Erro ao acessar {url_edicao}: {e}")
        return artigos
    
    soup = BeautifulSoup(r.text, 'html.parser')
    divs_artigos = soup.find_all('div', class_='artigosLista')
    for div in divs_artigos:
        h3_titulo = div.find('h3')
        if not h3_titulo:
            continue
        link_titulo = h3_titulo.find('a', class_='tituloArtigo')
        if not link_titulo:
            continue
        titulo = link_titulo.text.strip()
        titulo_norm = normalizar_titulo(titulo)
        total_access = None
        
        p_tags = div.find_all('p')
        for p in p_tags:
            txt = p.get_text(strip=True)
            if 'Total access:' in txt:
                match = re.search(r'Total access:\s*(\d+)', txt)
                if match:
                    total_access = int(match.group(1))
                break
        artigos[titulo_norm] = total_access
    return artigos

"""
Realiza o scraping da página de edições anteriores, da JBCS no portal SBQ

Cada chave do dicionário é uma tupla (ano, volume) (ambos inteiros) e o valor é uma lista de
tuplas, onde cada tupla contém:
    - O número da edição (str) (por exemplo, "1", "2", "3a", etc)
    - A URL completa para acessar essa edição

Retorna:
    dict: { (ano: int, volume: int): [(numero_edição: str, url_edição: str), ...] }
            Se ocorrer um erro ou a tabela não for encontrada, retorna um dicionário vazio
"""
def get_edicoes_anteriores_jbcs():
    print("[JBCS] Iniciando o scraping das edições anteriores (JBCS)...")
    base_url = 'https://jbcs.sbq.org.br/'
    url = base_url + 'past_issues'
    try:
        r = requests.get(url)
        r.raise_for_status()
    except Exception as e:
        print(f"[JBCS] Erro ao acessar {url}: {e}")
        return {}
    
    soup = BeautifulSoup(r.text, 'html.parser')
    edicoes_dict = {}
    tabela = None
    for table in soup.find_all('table'):
        if table.find('a', href=re.compile(r'edicoes_anteriores\.asp\?ano=')):
            tabela = table
            break
    
    if not tabela:
        print("[JBCS] Tabela de edições não encontrada!")
        return edicoes_dict
    
    trs = tabela.find_all('tr')
    for tr_idx, tr in enumerate(trs, start=1):
        tds = tr.find_all('td')
        if len(tds) < 4:
            continue
        try:
            ano_str = tds[1].get_text(strip=True)
            vol_str = tds[2].get_text(strip=True)
            ano = int(ano_str)
            vol = int(vol_str)
        except:
            continue
        
        edicoes_list = []
        for td_edicao in tds[3:]:
            a = td_edicao.find('a')
            if a:
                numero_site = a.get_text(strip=True)
                numero_site_clean = numero_site.replace('*', '').strip()
                href = a.get('href', '')
                edicoes_list.append((numero_site_clean, base_url + href))
        edicoes_dict[(ano, vol)] = edicoes_list
    return edicoes_dict

"""
Extrai os artigos de uma edição específica do JBCS a partir da URL fornecida

Para cada artigo encontrado, a função retorna um dicionário onde a chave é o título
normalizado e o valor é o valor de "Total access"

Parâmetros:
    url_edicao (str): URL completa da edição do JBCS 

Retorna:
    dict: { titulo_normalizado (str): total_access (int ou None) }
    Se ocorrer erro durante o acesso, retorna um dicionário vazio
"""
def get_artigos_de_uma_edicao_jbcs(url_edicao):
    print(f"[JBCS] Buscando artigos em {url_edicao}")
    artigos = {}
    try:
        r = requests.get(url_edicao)
        r.raise_for_status()
    except Exception as e:
        print(f"[JBCS] Erro ao acessar {url_edicao}: {e}")
        return artigos
    
    soup = BeautifulSoup(r.text, 'html.parser')
    divs_artigos = soup.find_all('div', class_='artigosLista')
    for div in divs_artigos:
        h3_titulo = div.find('h3')
        if not h3_titulo:
            continue
        link_titulo = h3_titulo.find('a', class_='tituloArtigo')
        if not link_titulo:
            continue
        titulo = link_titulo.text.strip()
        titulo_norm = normalizar_titulo(titulo)
        total_access = None
        
        for p in div.find_all('p'):
            txt = p.get_text(strip=True)
            if 'Total access:' in txt:
                match = re.search(r'Total access:\s*(\d+)', txt)
                if match:
                    total_access = int(match.group(1))
                break
        artigos[titulo_norm] = total_access
    return artigos

"""
Carrega o CSV de entrada com dados básicos dos artigos, realiza o scraping do valor 
"Total Access" para os periódicos Química Nova e JBCS e gera um novo CSV de saída 
com a coluna 'TotalAccess' integrada

Parâmetros:
    input_csv (str): Caminho para o arquivo CSV de entrada ("articles.csv")
    output_csv (str): Caminho para o arquivo CSV de saída com a coluna 'TotalAccess'

Retorna:
    None
"""
def run_total_access(input_csv, output_csv):
    
    # Ler CSV
    with open(input_csv, 'r', encoding='utf-8', newline='') as fin:
        reader = csv.DictReader(fin)
        rows = list(reader)
        fieldnames = reader.fieldnames
    
    if 'TotalAccess' not in fieldnames:
        fieldnames.append('TotalAccess')
    
    # Obter edições
    edicoes_qn = get_edicoes_anteriores_qn()
    edicoes_jbcs = get_edicoes_anteriores_jbcs()
    
    cache_qn = {}
    cache_jbcs = {}
    
    for row in rows:
        journal = row.get('journal', '').strip().upper()
        year_str = row.get('year', '').strip()
        vol_str = row.get('volume', '').strip()
        num_str = row.get('edition_number', '').strip()
        title_str = row.get('title', '').strip()
        
        # Verifica se é QN ou JBCS
        if journal not in ['QN', 'JBCS']:
            row['TotalAccess'] = ''
            continue
        
        # Tenta converter
        try:
            ano = int(year_str)
            vol = int(vol_str)
        except:
            row['TotalAccess'] = ''
            continue
        
        titulo_norm_csv = normalizar_titulo(title_str)
        
        # Processa QN
        if journal == 'QN':
            ed_list = edicoes_qn.get((ano, vol), [])
            url_edition = None
            for (numero, link_ed) in ed_list:
                if numero.strip() == num_str:
                    url_edition = link_ed
                    break
            if not url_edition:
                row['TotalAccess'] = ''
                continue
            
            key = (ano, vol, num_str)
            if key not in cache_qn:
                time.sleep(1)
                artigos_qn = get_artigos_de_uma_edicao_qn(url_edition)
                cache_qn[key] = artigos_qn
            else:
                artigos_qn = cache_qn[key]
            
            total_access = artigos_qn.get(titulo_norm_csv, '')
            row['TotalAccess'] = str(total_access)
        
        # Processa JBCS
        elif journal == 'JBCS':
            ed_list = edicoes_jbcs.get((ano, vol), [])
            url_edition = None
            for (numero_site, link_ed) in ed_list:
                if numero_site.strip() == num_str:
                    url_edition = link_ed
                    break
            if not url_edition:
                row['TotalAccess'] = ''
                continue
            
            key = (ano, vol, num_str)
            if key not in cache_jbcs:
                time.sleep(1)
                artigos_jbcs = get_artigos_de_uma_edicao_jbcs(url_edition)
                cache_jbcs[key] = artigos_jbcs
            else:
                artigos_jbcs = cache_jbcs[key]
            
            total_access = artigos_jbcs.get(titulo_norm_csv, '')
            row['TotalAccess'] = str(total_access) if total_access else ''
    
    # Salvar output
    with open(output_csv, 'w', encoding='utf-8', newline='') as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)