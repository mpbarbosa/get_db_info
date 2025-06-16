from app.dbconn import dbconnfactory
from app.dbconn.dbconnfactory import DatabaseFactory


class DatabaseStruct:
    def __init__(self, db_type: str, db_conn_data):
        self.db_type = db_type
        self.db_conn_data = db_conn_data

    def get_tables(self):
        dbconn = DatabaseFactory().create_connection(self.db_type, self.db_conn_data)
        # dbconn.executeQuery()
        dbconn.close()
