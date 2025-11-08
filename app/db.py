from sqlalchemy import create_engine, text
from config import Config

# Creating a global SQLAlchemy engine
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)

def run_query(query, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        rows = [dict(r._mapping) for r in result]
    return rows
