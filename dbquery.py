import oracledb
import pandas as pd
import psycopg2
import sqlalchemy
from pythonping import ping
from config import load_config
import terminal
from dbconnect.dbconfig import DbConfig
import dbconnect.dbconnect as dbconn
from dbconnect.sqlalchemy_connection import SqlalchemyConn


def connect_database(database=None,databasename=None,lib=None,exec_lvl=0):
    terminal.print_debug(txt=f'Inicio da funcao dbquery.connect_database(databae={database},databaname={databasename},lib={lib})')
    dbconfig = DbConfig(filename='database.ini',section=databasename)
    config = dbconfig.load_dbconfig()
    host = config['host']

    terminal.print_monitor('-')
    terminal.print_monitor('Ping')
    terminal.print_monitor(ping(host))
    terminal.print_monitor('-')

    conn = dbconn.open_dbconnection(database=database,dbconfig=dbconfig,conn_method=lib)

    if (conn == None):
        terminal.print_error("Conexao com o banco de dados nao foi criada.")
        terminal.print_warning("Saindo...")
        exit()
    return conn

def execute_query(sql_query_entry:tuple=None,sql_query=None,params=[],database=None,databasename=None,lib=None,exec_lvl=0):
    if (terminal.level < exec_lvl):
        return

    conn = connect_database(database=database,databasename=databasename,lib=lib,exec_lvl=exec_lvl)

    config = load_config(section=databasename)
    schema = config.get('schema','')

    tmp_sql_query = None
    if (sql_query_entry == None):        
        tmp_sql_query = sql_query
    else:
        sql_config = load_config('queries.ini',sql_query_entry)
        tmp_sql_query = sql_config['sql']
        

    final_sql_query = tmp_sql_query.replace(r'??esquema??',schema)
    result = None
    try:
        terminal.print_debug("Executing query...")
        result = conn.execute_sql(final_sql_query,parameters=params)
    except (oracledb.Error, TypeError) as error:
        terminal.print_debug(f"Tipo do cursor: {type(cursor)}")
        terminal.print_debug(f'Classe do erro: {type(error)}')
        error_msg = str(error)
        terminal.print_error(f'Error executing SQL: {error_msg}')
        terminal.print_debug(final_sql_query)
        terminal.print_debug(f"Parametros do SQL: {params}")
        raise error

    return result

def get_df_sql(sql_query_entry:tuple=None,sql_query=None,params=[],database=None,databasename=None,lib=None,exec_lvl=0):
    terminal.print_debug(txt=f'Inicio da funcao dbquery.get_df_sql(sql_query_entry={sql_query_entry},sql_query={sql_query},databae={database},databaname={databasename},lib={lib})')
    if (terminal.level < exec_lvl):
        return

    conn = connect_database(database=database,databasename=databasename,lib=lib,exec_lvl=exec_lvl)

    config = load_config(section=databasename)
    schema = config.get('schema','')

    tmp_sql_query = None
    if (sql_query_entry == None):        
        tmp_sql_query = sql_query
    else:
        sql_config = load_config('queries.ini',sql_query_entry)
        tmp_sql_query = sql_config['sql']
        

    final_sql_query = tmp_sql_query.replace(r'??esquema??',schema)

    df_result = None
    # Fetch results (if applicable)
    try:
        df_result = conn.get_sql_result_as_data_frame(final_sql_query=final_sql_query,parameters=params)
    except (oracledb.Error,psycopg2.ProgrammingError) as error:
        print(final_sql_query)
        error_msg = str(error)
        if ('DPY-1003' in error_msg):
            #oracledb.exceptions.InterfaceError: DPY-1003: the executed statement does not return rows
            print('Error executing SQL:', error_msg)
            df_result - pd.DataFrame([])
            raise error
        else:
            error_msg = str(error)
    terminal.print_debug(f"Dataframe de retorno: {df_result.shape}")
    return df_result

def get_df_sql2(sql_query_entry:tuple=None,sql_query=None,params=[],database=None,databasename=None,lib=None,exec_lvl=0):
    terminal.print_debug(txt=f'Inicio da funcao dbquery.get_df_sql2(sql_query_entry={sql_query_entry},sql_query={sql_query},databae={database},databaname={databasename},lib={lib})')
    if (terminal.level < exec_lvl):
        return

    dbconfig = DbConfig(filename='database.ini',section=databasename)
    config = dbconfig.load_dbconfig()
    user = config['user']
    password = config['password']
    host = config['host']
    port = config['port']
    service_name = config['service']
    dsn = f'{host}:{port}/{service_name}'

    conn = oracledb.connect(user=user, password=password, dsn=dsn)

    with conn.cursor() as cursor:
        cursor.execute(sql_query)

    
def executa_ddl(databasename=None,ddl=None):
    sql_config = load_config('queries.ini',ddl)

    config_databasename = sql_config['databasename']

    final_databasename = None
    if (config_databasename == 'default'):
        final_databasename = databasename
    else:
        final_databasename = config_databasename

    database = sql_config['database']
    conn_lib = sql_config['conn_lib']

    execute_query(sql_query_entry=ddl, database=database, databasename=final_databasename, lib=conn_lib)


def insert_df(df,database='postgresql',databasename=None,tablename=None,if_exists='append',lib='sqlalchemy'):
    terminal.print_debug(txt=f'Inicio da funcao dbquery.insert_df')
    config = load_config(section=databasename)
    conn = connect_database(database=database,databasename=databasename,lib=lib,exec_lvl=0)
    terminal.print_debug(f"Criada a conexao: {conn}, type: {type(conn)}")
    
    schema = None
    if (database=='postgresql'):
        schema = 'public'
    else:
        schema = config['schema']
    r = None
    connection = conn.connect()
    terminal.print_debug(f"Connection: {connection}, type: {type(connection)}")
    r = df.to_sql(name=tablename, con=connection,schema=schema,if_exists=if_exists,index=False,chunksize=5000)
    terminal.print_debug(f"Resultado do insert: {r}")
    conn.commit()
    conn.close()
        

def truncate_table(databasename='dados_ffc',database='oracle',tablename='tmp_san',lib='sqlalchemy'):
    config = load_config(section=databasename)
    schema = config.get('schema','')
    user = config['user']
    password = config['password']
    host = config['host']
    service = config.get('service',None)
    port = config['port']
    dsn = f'{host}:{port}/{service}'
    conn = SqlalchemyConn(user=user,password=password,host=host,port=port,service_name=service,database=database)
    engine = conn.create_dbengine()
    try:
        with engine.begin() as conn:
            terminal.print_debug('Executing sql...')
            sql = f"TRUNCATE TABLE {tablename}"
            terminal.print_debug(f'SQL: {sql}')
            conn.exec_driver_sql(sql)
    except sqlalchemy.exc.OperationalError as error:
        terminal.print_error(f"Erro ao tentar conectar banco: {conn}")
        raise error
        



def drop_table(databasename='dados_ffc',database='oracle',tablename='tmp_san',lib='sqlalchemy'):
    terminal.print_debug(txt=f'Inicio da funcao terminal.drop_table(databasename={databasename},database={database},tablename={tablename},lib={lib})')
    config = load_config(section=databasename)
    schema = config.get('schema','')
    user = config['user']
    password = config['password']
    host = config['host']
    service = config.get('service',None)
    port = config['port']
    conn = SqlalchemyConn(user=user,password=password,host=host,port=port,service_name=service,database=database)
    engine = conn.create_dbengine()
    with engine.begin() as conn:
        terminal.print_debug('Executing sql...')
        sql = f"DROP TABLE {tablename}"
        terminal.print_debug(f'SQL: {sql}')
        conn.exec_driver_sql(sql)

def fetchone(sql_query_entry:tuple=None,sql_query=None,params=[],database=None,databasename=None,lib=None,exec_lvl=0):
    if (terminal.level < exec_lvl):
        return

    config = load_config(section=databasename)
    schema = config.get('schema','')

    conn = connect_database(database=database,databasename=databasename,lib=lib,exec_lvl=exec_lvl)

    #ToDo: Fatorar o código abaixo
    tmp_sql_query = None
    if (sql_query_entry == None):        
        tmp_sql_query = sql_query
    else:
        sql_config = load_config('queries.ini',sql_query_entry)
        tmp_sql_query = sql_config['sql']
        

    final_sql_query = tmp_sql_query.replace(r'??esquema??',schema)

    return conn.fetchone(final_sql_query=final_sql_query,parameters=params)