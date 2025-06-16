from app.dbconn.dbconnect import MySQLConnection, SQLiteConnection


class DatabaseFactory:
    def create_connection(self, db_type, **kwargs):
        if db_type == "mysql":
            return MySQLConnection(**kwargs)
        elif db_type == "sqlite":
            return SQLiteConnection(**kwargs)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
