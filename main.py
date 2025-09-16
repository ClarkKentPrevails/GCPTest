from flask import Flask, Response
import json
import nltk
from gdelt_get_export import (
    get_gdelt_export_update_url,
    get_export_file_content,
    unzip_in_memory,
    csv_to_json,
    get_article_details
)

app = Flask(__name__)



@app.route('/download', methods=['GET'])
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
    
    return Response(
        json_data,
        mimetype='application/json',
        headers={
            "Content-Disposition": "attachment;filename=gdelt_export.json"
        }
    )

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)