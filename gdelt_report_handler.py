import requests
import json
import requests
import zipfile
import requests
import zipfile
import io
import pandas as pd
import numpy as np

from google.cloud import storage
from newspaper import Article
from gdelt_column_names import gdelt_column_names 
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage


GDELT_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def get_file_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content  

def unzip_in_memory(zipped_content):
    with zipfile.ZipFile(io.BytesIO(zipped_content), 'r') as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as f:
                return f.read()

def csv_to_json(csv_text: str, limit: int = None, report_type: str = "export"):
    column_names = gdelt_column_names[report_type]
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
        
def get_article_details(json_data: str, source_url: str = "Source_URL"):
    records_with_urls = [(i, record[source_url]) for i, record in enumerate(json_data) if record.get(source_url)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(extract_article_info, url): i for i, url in records_with_urls}
        for future in as_completed(futures):
            i = futures[future]
            json_data[i]["extracted_news"] = future.result()

    for record in json_data:
        if "extracted_news" not in record:
            record["extracted_news"] = None

    return json_data

def get_article_url_field(report_type: str) -> str:
    report_type = report_type.lower()
    
    if report_type == "export":
        return "Source_URL"
    elif report_type == "mentions":
        return "MentionIdentifier"
    elif report_type == "gkg":
        return "V2DOCUMENTIDENTIFIER"
    else:
        raise ValueError(f"Unknown report_type '{report_type}'. Must be one of 'export', 'mentions', or 'gkg'.")

def get_gdelt_update_urls():

    response = requests.get(GDELT_UPDATE_URL)
    response.raise_for_status()
    lines = [line.strip() for line in response.text.strip().splitlines() if line.strip()]
    
    if not lines:
        raise ValueError("No lines found in update file.")

    return lines

def get_report_url(update_urls, data_type) -> str:
    data_type = data_type.lower()
    valid_types = {"export", "mentions", "gkg"}

    if data_type not in valid_types:
        raise ValueError(f"Unknown data_type '{data_type}'. Must be one of {valid_types}")

    if isinstance(update_urls, str):
        lines = update_urls.strip().splitlines()
    else:
        lines = update_urls

    for line in lines:
        if line:
            parts = line.strip().split()
            if len(parts) == 3:
                url = parts[2]
                if f".{data_type}." in url.lower():
                    return url

    return ""

def get_report(url: str, report_type: str):
    zipped_content = get_file_content(url)
    csv_content = unzip_in_memory(zipped_content)
    json_content = csv_to_json(csv_content.decode('utf-8'), 50, report_type)
    enriched_json = get_article_details(json_content, get_article_url_field(report_type))
    json_data = json.dumps(enriched_json, ensure_ascii=False, indent=2)
    
    return json_data


def load_to_gcs(json_data: str, bucket_name: str, filename: str):
    storage_client = storage.Client()
    storage_bucket = storage_client.bucket(bucket_name)
    storage_blob = storage_bucket.blob(filename)
    storage_blob.upload_from_string(json_data, content_type='application/json')

    return f"gs://{bucket_name}/{filename}"