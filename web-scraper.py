import requests
from bs4 import BeautifulSoup
import time
import random
import re
import logging
import csv
from datetime import datetime
import concurrent.futures
from requests.adapters import HTTPAdapter

# Configurações Gerais
BASE_URL = "https://www.scielo.br"
URL_ISSUES = "https://www.scielo.br/j/qn/grid"

# Intervalo de espera entre requisições (em segundos)
MIN_WAIT = 1.0
MAX_WAIT = 3.0

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,  # Altere para DEBUG para mais detalhes
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ScraperQuimicaNova")

# Funções Auxiliares
def random_sleep():
    """ Aguarda um tempo aleatório entre MIN_WAIT e MAX_WAIT """
    time.sleep(random.uniform(MIN_WAIT, MAX_WAIT))

def create_session():
    """
    Cria e retorna um objeto requests.Session() com headers configurados.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/110.0.0.0 Safari/537.36")
    })
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    return session

def parse_date_yyyymmdd(date_str):
    """
    Converte uma string no formato YYYYMMDD para um objeto datetime.
    Retorna None se a conversão falhar.
    """
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return None

def get_soup(session, url):
    """
    Faz GET na URL usando a session fornecida e retorna um objeto BeautifulSoup.
    """
    logger.debug(f"Requisitando: {url}")
    random_sleep()
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

# Extração de Dados
def extract_issues_links(session, url=URL_ISSUES):
    """
    Extrai a lista de anos/volumes e os links das edições a partir da página de grid.
    Retorna uma lista de dicionários.
    """
    logger.info(f"Extraindo issues (anos/volumes) de {url}")
    soup = get_soup(session, url)
    
    table = soup.find("table", class_="table-hover")
    if not table:
        logger.warning("Não foi possível localizar a tabela de volumes/edições.")
        return []
    
    result = []
    tbody = table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr", recursive=False)
    else:
        rows = table.find_all("tr", recursive=False)
    
    for row in rows:
        cols = row.find_all(["td", "th"])
        if len(cols) < 3:
            continue
        year = cols[0].get_text(strip=True)
        volume = cols[1].get_text(strip=True)
        edition_cell = cols[2]
        edition_links = [a.get("href") for a in edition_cell.find_all("a", href=True)]
        result.append({
            "year": year,
            "volume": volume,
            "edition_links": edition_links
        })
    
    logger.info(f"Foram encontrados {len(result)} anos/volumes.")
    return result

def extract_articles_from_edition(session, edition_url):
    """
    Dado um link de edição (ex.: "/j/qn/i/2025.v48n1/"), extrai os artigos da edição.
    Retorna uma lista de dicionários com os dados extraídos.
    """
    full_url = BASE_URL + edition_url
    logger.debug(f"Extraindo artigos da edição: {full_url}")
    soup = get_soup(session, full_url)
    
    articles_table = soup.find("table", class_="table-journal-list")
    if not articles_table:
        logger.warning(f"Não foi encontrada a table-journal-list em {full_url}")
        return []
    
    tbody = articles_table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr", recursive=False)
    else:
        rows = articles_table.find_all("tr", recursive=False)
    
    articles_data = []
    for row in rows:
        td = row.find("td", attrs={"data-date": True})
        if not td:
            continue
        
        publication_date_str = td.get("data-date", "")
        publication_date = parse_date_yyyymmdd(publication_date_str)
        
        pub_type_span = td.find("span", class_=re.compile("badge-info"))
        publication_type = pub_type_span.get_text(strip=True) if pub_type_span else None
        
        title_strong = td.find("strong")
        title = title_strong.get_text(" ", strip=True) if title_strong else None
        
        pid_comment = td.find(string=lambda text: text and "PID:" in text)
        pid = None
        if pid_comment:
            match = re.search(r'PID:\s+([\w-]+)', pid_comment)
            if match:
                pid = match.group(1)
        
        author_links = td.find_all("a", href=re.compile("q=au:"))
        authors = [a.get_text(strip=True) for a in author_links]
        
        nav_items = td.find("ul", class_="nav")
        abstract_link, text_link, pdf_link = None, None, None
        if nav_items:
            li_items = nav_items.find_all("li", class_="nav-item")
            for li in li_items:
                label_strong = li.find("strong")
                if not label_strong:
                    continue
                label_lower = label_strong.get_text(strip=True).lower()
                link_tag = li.find("a")
                if link_tag:
                    link_href = link_tag.get("href")
                    full_link = BASE_URL + link_href
                    # Verifica se o label contém termos que identifiquem o link
                    if any(word in label_lower for word in ['abstract', 'resumo', 'summary']):
                        abstract_link = full_link
                        if '?lang=' not in abstract_link:
                            abstract_link += "&lang=en"
                    elif any(word in label_lower for word in ['text', 'texto', 'full text']):
                        text_link = full_link
                    elif any(word in label_lower for word in ['pdf']):
                        pdf_link = full_link
        article_info = {
            "edition_url": edition_url,
            "publication_date": publication_date,
            "publication_type": publication_type,
            "title": title,
            "authors": authors,
            "pid": pid,
            "abstract_link": abstract_link,
            "text_link": text_link,
            "pdf_link": pdf_link,
        }
        articles_data.append(article_info)
    
    logger.info(f"Encontrados {len(articles_data)} artigos na edição {edition_url}")
    return articles_data

def extract_keywords_etc(session, abstract_url):
    """
    Acessa o link do abstract para extrair as keywords.
    Retorna uma lista com as palavras-chave.
    """
    if not abstract_url:
        return []
    
    logger.debug(f"Extraindo keywords de: {abstract_url}")
    soup = get_soup(session, abstract_url)
    keywords = []
    paragraphs = soup.find_all("p")
    for p in paragraphs:
        strong = p.find("strong")
        if strong is None:
            continue
        text_in_strong = strong.get_text(strip=True).lower()
        if any(term in text_in_strong for term in ["keyword", "palavras-chave", "palabras clave"]):
            text_full = p.get_text(strip=True)
            # Remove os termos iniciais, independentemente do idioma
            text_clean = re.sub(r"(?i)^(keywords|palavras-chave|palabras clave):\s*", "", text_full)
            splits = re.split(r"[;,\.]\s*", text_clean)
            for kw in splits:
                kw_clean = kw.strip()
                if kw_clean and not kw_clean.lower().startswith("keyword"):
                    keywords.append(kw_clean)
    return keywords

def extract_institutions_from_article_page(session, abstract_url):
    """
    Faz parse das divs de modal que contêm informações das instituições.
    Retorna uma lista com as descrições das instituições.
    """
    if not abstract_url:
        return []
    logger.debug(f"Extraindo instituições de: {abstract_url}")
    soup = get_soup(session, abstract_url)
    institutions = []
    modals = soup.find_all("div", class_="modal-body")
    for modal in modals:
        aff_spans = modal.find_all("span", attrs={"data-aff-display": True})
        for span in aff_spans:
            inst_text = span.get_text(" ", strip=True)
            if inst_text and inst_text not in institutions:
                institutions.append(inst_text)
    return institutions

# Processamento de Cada Edição (usando concorrência)
def process_edition(ed_link, year, volume, session):
    articles = extract_articles_from_edition(session, ed_link)
    for art in articles:
        art["year"] = year
        art["volume"] = volume
        kws = extract_keywords_etc(session, art["abstract_link"])
        art["keywords"] = kws
        insts = extract_institutions_from_article_page(session, art["abstract_link"])
        art["institutions"] = insts
    return articles

# Pipeline Principal
def main():
    session = create_session()
    issues_info = extract_issues_links(session, URL_ISSUES)
    all_articles = []
    
    # Processa edições em paralelo 
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_to_edition = {}
        for issue in issues_info:
            year = issue["year"]
            volume = issue["volume"]

            for ed_link in issue["edition_links"][:2]: # '[:2]' somente para limitacao de testes, no real deve ser retirado para pegar tudo
                future = executor.submit(process_edition, ed_link, year, volume, session)
                future_to_edition[future] = ed_link
        for future in concurrent.futures.as_completed(future_to_edition):
            ed = future_to_edition[future]
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as exc:
                logger.error(f"Erro ao processar edição {ed}: {exc}")
    
    # Salvando os resultados em CSV
    if all_articles:
        fieldnames = [
            "year", "volume", "edition_url", "publication_date",
            "publication_type", "title", "authors", "pid",
            "abstract_link", "text_link", "pdf_link", "keywords", "institutions"
        ]
        with open("quimicanova_articles.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for article in all_articles:
                article["authors"] = "; ".join(article["authors"]) if article["authors"] else ""
                article["keywords"] = "; ".join(article["keywords"]) if article["keywords"] else ""
                article["institutions"] = "; ".join(article["institutions"]) if article["institutions"] else ""
                if isinstance(article["publication_date"], datetime):
                    article["publication_date"] = article["publication_date"].strftime("%Y-%m-%d")
                writer.writerow(article)
        logger.info(f"Finalizado! Total de artigos extraídos: {len(all_articles)}")
        logger.info("Arquivo CSV 'quimicanova_articles.csv' criado com sucesso.")
    else:
        logger.info("Nenhum artigo foi encontrado. Verifique se houve problema na extração.")

if __name__ == "__main__":
    main()
