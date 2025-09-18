from flask import Flask, jsonify
import nltk
import datetime
import os

from gdelt_report_handler import (
    get_gdelt_update_response,
    get_export_update,
    get_update_url,
    load_export_to_gcs
)

app = Flask(__name__)

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "gdelt_export_data")

@app.route('/', methods=['GET'])
def download_gdelt_data():
    try:
        nltk.data.path.append("/opt/nltk_data")
        nltk.download('punkt', download_dir="/opt/nltk_data")
        nltk.download('punkt_tab', download_dir="/opt/nltk_data")
        
        gdelt_update_urls = get_gdelt_update_response()
    
        # export
        export_json_data = get_export_update(get_update_url(gdelt_update_urls, data_type="export"))
        export_filename = f"export_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        load_export_to_gcs(export_json_data, BUCKET_NAME, export_filename)
        
        # mentions
        mention_json_data = get_export_update(get_update_url(gdelt_update_urls, data_type="mention"))
        mention_filename = f"mention_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        load_export_to_gcs(mention_json_data, BUCKET_NAME, mention_filename)
        
        # gkg


        return jsonify({
            "status": "success",
            "bucket": BUCKET_NAME,
            "export_filename": export_filename,
            "export_gcs_url": f"gs://{BUCKET_NAME}/{export_filename}",
            "mention_filename": mention_filename,
            "mention_gcs_url": f"gs://{BUCKET_NAME}/{mention_filename}"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)