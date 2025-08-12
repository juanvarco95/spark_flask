import os
from flask import Flask
from .config.settings import Settings
from .routes.csv_routes import csv_bp

def create_app():
    app = Flask(__name__)
    
    app.config.from_object(Settings)
    
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok = True)
    
    app.register_blueprint(csv_bp)
    
    return app  