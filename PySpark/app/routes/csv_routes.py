import os
from werkzeug.utils import secure_filename
from flask import (Blueprint, 
                current_app, 
                request, 
                render_template_string, 
                redirect,
                url_for)
from ..services.csv_service import (save_uploaded_file,
                                    read_csv_to_spark_df,
                                    df_to_html)

csv_bp = Blueprint('csv', __name__)

@csv_bp.route('/')
def index():
    return """ 
    <h1>Upload CSV to Pyspark</h1>
    <form method="POST" action="/upload" enctype="multipart/form-data">
        <input type="file" name="file" accept=".csv" required>
        <input type="submit" value="Upload">
    </form>
    """
    
@csv_bp.route('/upload', methods=["POST"])
def upload():
    if 'file' not in request.files:
        return 'No file uploaded', 400
    
    file = request.files['file']
    if file.filename == '':
        return 'No file selected', 400
    
    filename = secure_filename(file.filename)
    upload_folder = current_app.config['UPLOAD_FOLDER']
    
    saved_path = save_uploaded_file(file, upload_folder, filename)
    
    df = read_csv_to_spark_df(saved_path)
    
    html_table = df_to_html(df)
    
    html = f"""
    <html>
    <head>
        <title>Uploaded CSV</title>
        <style>
            table {{ border-collapse: collapse; width: 80%; margin: 20px auto; }}
            th, td {{ border: 1px solid #444; padding: 6px; text-align: left; }}
            th {{ background: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2 style="text-align:center;">CSV Data</h2>
        {html_table}
        <p style="text_align:center;"><a href="/">Upload another</a></p>
    </body>
    </html>
    """
    
    return render_template_string(html)  

