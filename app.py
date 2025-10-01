"""Flask application initialization."""
from flask import Flask
from config import Config
from shared.db import init_db
from features.property_table.routes import bp as property_table_bp

def create_app(config_class=Config):
    """Create and configure Flask app."""
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Initialize database
    init_db(app)
    
    # Register blueprints
    app.register_blueprint(property_table_bp)
    
    # Root route
    @app.route('/')
    def index():
        """Redirect to property table."""
        from flask import redirect, url_for
        return redirect(url_for('property_table.list_properties'))
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
