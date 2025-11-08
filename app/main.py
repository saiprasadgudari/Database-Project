from flask import Flask, jsonify
from flask_cors import CORS
from db import run_query

def create_app():
    app = Flask(__name__)
    CORS(app) 

    @app.route("/")
    def index():
        return jsonify({"message": "NYC Taxi Database API running..."})

    # Health check route
    @app.route("/api/health")
    def health():
        try:
            run_query("SELECT 1;")
            return jsonify({"status": "ok"})
        except Exception as e:
            return jsonify({"status": "error", "details": str(e)}), 500

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
