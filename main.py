from flask import Flask, jsonify
import nltk
import datetime
import os

from gdelt_report_handler import (
    get_gdelt_update_urls,
    get_report_url,
    get_report,
    load_to_gcs
)

app = Flask(__name__)

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "gdelt_reports")

@app.route('/<report_type>', methods=['GET'])
def download_gdelt_data(report_type):
    try:
        nltk.data.path.append("/opt/nltk_data")
        nltk.download('punkt', download_dir="/opt/nltk_data")
        nltk.download('punkt_tab', download_dir="/opt/nltk_data")
        
        valid_report_types = {"export", "mentions", "gkg"}
        report_type = report_type.lower()
        
        if report_type not in valid_report_types:
            raise ValueError(f"Unknown data_type '{report_type}'. Must be one of {valid_report_types}")
        
        gdelt_update_urls = get_gdelt_update_urls()
        report_url = get_report_url(gdelt_update_urls, data_type=report_type)
        
        json_data = get_report(report_url, report_type)
        filename = f"{report_type}{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        load_to_gcs(json_data, BUCKET_NAME, filename)
        

        return jsonify({
            "status": "success",
            "bucket": BUCKET_NAME,
            "filename": filename,
            "gcs_url": f"gs://{BUCKET_NAME}/{filename}"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)