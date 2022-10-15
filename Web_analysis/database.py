from psycopg2 import connect
from config import DB_CONFIG
from sqlalchemy import create_engine


class PostgresDB:
    def __init__(self):
        self.conn = connect(dbname=DB_CONFIG['database'], user=DB_CONFIG['db_user'],
                            password=DB_CONFIG['password'], host=DB_CONFIG['host'])
        self.conn.autocommit = True

    def select(self, query: str) -> list:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
        return records

    def insert(self, query: str) -> str:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
        return "database updated"

    def close(self):
        return self.conn.close()
