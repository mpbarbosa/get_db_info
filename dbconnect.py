import oracledb
from .database_connect import DatabaseConn, OracleConn
from .sqlalchemy_connection import SqlalchemyConn
from .database_connect import PostgresqlConn
import psycopg2
from config import load_config
import terminal
from .dbconfig import DbConfig



def open_postgresql_dbconnection(dbconfig:DbConfig=None,conn_method:str=None):
    terminal.print_debug(txt=f'Inicio funcao dbconnect.open_postgresql_dbconnection(dbconfig: {dbconfig}')
    config = dbconfig.config
    user = config['user']
    password = config['password']
    host = config['host']
    port = config['port']
    service_name = config.get('service',None)
    conn = None
    if (conn_method == 'psycopg2'):
        config =load_config(filename='database.ini',section='postgresql')
        try:
            conn = PostgresqlConn(user=user,password=password,host=host,port=port,service_name=service_name,conn_method=conn_method)
        except (psycopg2.DatabaseError, Exception) as error:
            terminal.print_terminal(error)
            raise error
    elif (conn_method == 'sqlalchemy'):
        config = dbconfig.config
        user = config['user']
        password = config['password']
        host = config['host']
        service_name = config.get('service',None)
        port = config['port']
        conn = None
        conn = SqlalchemyConn(user=user,password=password,host=host,database='postgresql')
    return conn

def open_oracle_dbconnection(dbconfig:DbConfig=None,conn_method:str=None) -> DatabaseConn:
    terminal.print_debug(txt=f'Inicio funcao dbconnect.open_oracle_dbconnection(dbconfig: {dbconfig}')
    config = dbconfig.config
    user = config['user']
    password = config['password']
    host = config['host']
    service_name = config.get('service',None)
    port = config['port']
    conn: DatabaseConn = None
    if (conn_method=='oracledb'):
        conn = OracleConn(user=user,password=password,host=host,port=port,service_name=service_name,conn_method=conn_method)
    elif (conn_method=='sqlalchemy'):
        conn = SqlalchemyConn(user=user,password=password,host=host,port=port,service_name=service_name,database='oracle')
    terminal.print_debug(txt=f'Conexao Oracle criada: {conn}')
    conn.connect()
    return conn
    

def open_dbconnection(dbconfig:DbConfig=None,database=None,conn_method=None):
    conn = None
    if (database=='oracle'):
        conn = open_oracle_dbconnection(dbconfig=dbconfig,conn_method=conn_method)
    elif (database == 'postgresql'):
        conn = open_postgresql_dbconnection(dbconfig=dbconfig,conn_method=conn_method)
    else:
        print("Database invalido")
        exit()
    return conn


def close_dbconnection(conn):
    if conn:
        conn.close()
        terminal.print_monitor('Database connection closed.')