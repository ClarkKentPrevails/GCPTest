from flask import Flask, Response
from google.cloud import storage
import json
import nltk
import datetime
import os
from gdelt_get_export import (
    get_gdelt_export_update_url,
    get_export_file_content,
    unzip_in_memory,
    csv_to_json,
    get_article_details
)

app = Flask(__name__)

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "gdelt_export_data")

@app.route('/', methods=['GET'])
def download_gdelt_data():
    nltk.data.path.append("/opt/nltk_data")
    nltk.download('punkt', download_dir="/opt/nltk_data")
    nltk.download('punkt_tab', download_dir="/opt/nltk_data")
    
    export_url = get_gdelt_export_update_url()
    export_content = get_export_file_content(export_url)
    csv_content = unzip_in_memory(export_content)
    json_content = csv_to_json(csv_content.decode('utf-8'), limit=50)
    enriched_json = get_article_details(json_content)
    json_data = json.dumps(enriched_json, ensure_ascii=False, indent=2)
    
    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    filename = f"gdelt_export_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json_data, content_type='application/json')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)