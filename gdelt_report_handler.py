import requests
import json
from gdelt_get_export import (
    get_export_file_content,
    unzip_in_memory,
    csv_to_json,
    get_article_details
)

GDELT_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def get_gdelt_update_response():

    response = requests.get(GDELT_UPDATE_URL)
    response.raise_for_status()
    lines = [line.strip() for line in response.text.strip().splitlines() if line.strip()]
    
    if not lines:
        raise ValueError("No lines found in update file.")

    return lines


def get_update_url(update_urls: str, data_type: str = "export") -> str:
    data_type = data_type.lower()
    valid_types = {"export", "mentions", "gkg"}
    
    if data_type not in valid_types:
        raise ValueError(f"Unknown data_type '{data_type}'. Must be one of {valid_types}")

    for line in update_urls.strip().splitlines():
        if line:
            parts = line.strip().split()
            if len(parts) == 3:
                url = parts[2]
                if f".{data_type}." in url.lower():
                    return url
    
    return ""


def get_export_update(url: str):
    export_content = get_export_file_content(url)
    csv_content = unzip_in_memory(export_content)
    json_content = csv_to_json(csv_content.decode('utf-8'), limit=50)
    enriched_json = get_article_details(json_content)
    json_data = json.dumps(enriched_json, ensure_ascii=False, indent=2)
    
    return json_data


def load_export_to_gcs(json_data: str, bucket_name: str, filename: str):
    from google.cloud import storage
    import datetime

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(json_data, content_type='application/json')

    return f"gs://{bucket_name}/{filename}"