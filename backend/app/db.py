from sqlalchemy import create_engine, text
from app.config import Config

# Create global SQLAlchemy engine using URI from Config class
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)

def run_query(query, params=None):
    """Run a SQL query and return results as list of dicts."""
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        rows = [dict(r._mapping) for r in result]
    return rows
