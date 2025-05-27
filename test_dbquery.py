import unittest
from datetime import date
import pandas as pd
from . import database_connect
from . import dbquery as db
import terminal


class Test_dbquery(unittest.TestCase):

    def test_lib_oracledb(self):
        conn = db.connect_database(database='oracle',databasename='dados_ffc',lib='oracledb',exec_lvl=0)
        with conn.dbcursor() as cursor:
            cursor.execute('select sysdate from dual')
            self.assertTrue(str(type(cursor))=="<class 'oracledb.Cursor'>")
            data = cursor.fetchone()[0]
            hoje = data.date()
            self.assertTrue(date.today() == hoje)
            
    def test_lib_oracledb2(self):
        db.get_df_sql2(sql_query='select sysdate from dual',databasename='dados_ffc')

    def test_select_oracledb(self):
        df = db.get_df_sql(sql_query_entry='ORACLE_AGORA',database='oracle',databasename='dados_ffc',lib='oracledb')
        self.assertTrue(df['HOJE'][0].date() == date.today())


    def test_select_sqlalchemy(self):
        df = db.get_df_sql(sql_query_entry='ORACLE_AGORA',database='oracle',databasename='dados_ffc',lib='sqlalchemy')
        self.assertTrue(df['hoje'][0].date() == date.today())

    def test_select_postgres_sqlalchemy(self):
        df = db.get_df_sql(sql_query_entry='POSTGRES_TESTE',database='postgresql',databasename='postgresql',lib='sqlalchemy')
        self.assertTrue(df['hoje'][0].date() == date.today())

    def test_truncate_table_postgresql(self):
        db.truncate_table(database='postgresql',databasename='postgresql',tablename='teste_truncate_table',lib='sqlalchemy')

    def test_truncate_table_oracle(self):
        db.truncate_table(database='oracle',databasename='dados_ffc',tablename='teste_truncate_table',lib='sqlalchemy')

    def test_create_oracle_connection(self):
        conn = db.connect_database(database='oracle',databasename='dados_ffc',lib='oracledb',exec_lvl=0)

    def test_insert_df_into_table(self):
        df = pd.DataFrame({'name' : ['User P', 'User Q', 'User R']})
        db.insert_df(df=df,database='postgresql',databasename='postgresql',tablename='test_insert_df',if_exists='replace',lib='sqlalchemy')
        #ToDo: fazer o select e testar se o insert realmente aconteceu

        db.drop_table(databasename='postgresql',database='postgresql',tablename='test_insert_df',lib='sqlalchemy')

    def test_connect_psycopg2(self):
        conn = db.connect_database(database='postgresql',databasename='postgresql',lib='psycopg2')
        conn.connect()

    def test_postgresql(self):
        sql_max_id = 'select coalesce(max("IP_SERVICE_ORDER_ID"),0) as max_id from FT_SERVICE_ORDER_IDS'
        max_id_df_result = db.get_df_sql(sql_query=sql_max_id,database='postgresql',databasename='postgresql',lib='psycopg2') 

    def test_fetchone(self):
        result = db.fetchone(sql_query_entry='ORACLE_AGORA',sql_query=None,params=[],database='oracle',databasename='dados_ffc',lib='oracledb',exec_lvl=0)
        hoje = result[0].date()
        self.assertTrue(hoje == date.today())