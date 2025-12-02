from flask import Flask, jsonify
from flask_cors import CORS
from app.db import run_query



from app.routes.analytics import analytics_bp  
from app.routes.map_view import map_bp          
from app.routes.fare_trip import fare_tip_bp  
from app.routes.peak_hours import peak_bp
from app.routes.vendor_performace import vendor_bp 



def create_app():
    app = Flask(__name__)
    CORS(app)

    # Register Blueprints
    app.register_blueprint(analytics_bp)  
    app.register_blueprint(map_bp)        
    app.register_blueprint(fare_tip_bp)
    app.register_blueprint(peak_bp)
    app.register_blueprint(vendor_bp)



    # Root message
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
    app.run(host="0.0.0.0", port=5001, debug=True)

