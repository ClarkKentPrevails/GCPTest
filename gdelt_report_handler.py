import requests
import json
from google.cloud import storage

from gdelt_get_export import (
    get_export_file_content,
    export_unzip_in_memory,
    export_csv_to_json,
    export_get_article_details
)

from gdelt_get_mention import (
    get_mention_file_content,
    mention_unzip_in_memory,
    mention_csv_to_json,
    mention_get_article_details
)

from gdelt_get_gkg import (
    get_gkg_file_content,
    gkg_unzip_in_memory,
    gkg_csv_to_json,
    gkg_get_article_details
)

GDELT_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def get_gdelt_update_response():

    response = requests.get(GDELT_UPDATE_URL)
    response.raise_for_status()
    lines = [line.strip() for line in response.text.strip().splitlines() if line.strip()]
    
    if not lines:
        raise ValueError("No lines found in update file.")

    return lines


def get_update_url(update_urls, data_type: str) -> str:
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


def get_export_update(export_url: str):
    export_zipped_content = get_export_file_content(export_url)
    export_csv_content = export_unzip_in_memory(export_zipped_content)
    expoort_json_content = export_csv_to_json(export_csv_content.decode('utf-8'), limit=50)
    export_enriched_json = export_get_article_details(expoort_json_content)
    export_json_data = json.dumps(export_enriched_json, ensure_ascii=False, indent=2)
    
    return export_json_data


def load_export_to_gcs(export_json_data: str, export_bucket_name: str, export_filename: str):
    export_storage_client = storage.Client()
    export_bucket = export_storage_client.bucket(export_bucket_name)
    export_blob = export_bucket.blob(export_filename)
    export_blob.upload_from_string(export_json_data, content_type='application/json')

    return f"gs://{export_bucket_name}/{export_filename}"


def get_mention_update(url: str):
    mention_zipped_content = get_mention_file_content(url)
    mention_csv_content = mention_unzip_in_memory(mention_zipped_content)
    mention_json_content = mention_csv_to_json(mention_csv_content.decode('utf-8'), limit=50)
    mention_enriched_json = mention_get_article_details(mention_json_content)
    mention_json_data = json.dumps(mention_enriched_json, ensure_ascii=False, indent=2)
    
    return mention_json_data


def load_mention_to_gcs(mention_json_data: str, mention_bucket_name: str, mention_filename: str):
    mention_storage_client = storage.Client()
    mention_bucket = mention_storage_client.bucket(mention_bucket_name)
    mention_blob = mention_bucket.blob(mention_filename)
    mention_blob.upload_from_string(mention_json_data, content_type='application/json')

    return f"gs://{mention_bucket_name}/{mention_filename}"

def get_gkg_update(gkg_url: str):
    gkg_zipped_content = get_gkg_file_content(gkg_url)
    gkg_csv_content = gkg_unzip_in_memory(gkg_zipped_content)
    gkg_json_content = gkg_csv_to_json(gkg_csv_content.decode('utf-8'), limit=50)
    gkg_enriched_json = gkg_get_article_details(gkg_json_content)
    gkg_json_data = json.dumps(gkg_enriched_json, ensure_ascii=False, indent=2)
    
    return gkg_json_data


def load_gkg_to_gcs(gkg_json_data: str, gkg_bucket_name: str, gkg_filename: str):
    gkg_storage_client = storage.Client()
    gkg_bucket = gkg_storage_client.bucket(gkg_bucket_name)
    gkg_blob = gkg_bucket.blob(gkg_filename)
    gkg_blob.upload_from_string(gkg_json_data, content_type='application/json')

    return f"gs://{gkg_bucket_name}/{gkg_filename}"