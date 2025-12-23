import duckdb

class DuckDBManager:
    def __init__(self, path="data_sources/snapshots/latest.duckdb"):
        self.conn = duckdb.connect(path)

    def list_tables(self):
        return [row[0] for row in self.conn.execute("SHOW TABLES").fetchall()]

    def query(self, sql: str):
        return self.conn.execute(sql).fetchdf()
