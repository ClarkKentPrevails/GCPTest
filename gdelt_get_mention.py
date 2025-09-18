import requests
import zipfile
import io
import pandas as pd
import numpy as np
import json
from google.cloud import storage

from newspaper import Article
from gdelt_column_names import gdelt_column_names 
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_mention_file_content(mention_url):
    response = requests.get(mention_url)
    response.raise_for_status()
    return response.content  

def unzip_in_memory(zip_content):
    with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as f:
                return f.read()

def csv_to_json(csv_text: str, limit: int = None):
    column_names = gdelt_column_names["mentions"]
    df = pd.read_csv(io.StringIO(csv_text), delimiter='\t', header=None, names=column_names, engine='python')
    df = df.replace("NaN", np.nan)
    df.replace({np.nan: None}, inplace=True)
    
    if limit is not None:
        df = df.head(limit)
        
    records = df.to_dict(orient="records")
    
    return records
        
def extract_article_info(url):
    article = Article(url)
    try:
        article.download()
        article.parse()
        article.nlp()
        return {
            "news_title": article.title,
            "news_text": article.text,
            "news_publish_date": article.publish_date.isoformat() if article.publish_date else None,
            "news_authors": article.authors,
            "news_top_image": article.top_image,
            "news_summary": article.summary,
            "news_keywords": article.keywords
        }
    except Exception as e:
        return {"error": str(e)}
        
def get_article_details(json_data: str):
    records_with_urls = [(i, record["MentionIdentifier"]) for i, record in enumerate(json_data) if record.get("MentionIdentifier")]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(extract_article_info, url): i for i, url in records_with_urls}
        for future in as_completed(futures):
            i = futures[future]
            json_data[i]["extracted_news"] = future.result()

    for record in json_data:
        if "extracted_news" not in record:
            record["extracted_news"] = None

    return json_data

