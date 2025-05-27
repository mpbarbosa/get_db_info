import sqlalchemy
import pandas as pd
import terminal

class SqlalchemyConn():

    def __init__(self,user:str,password:str,host:str,port:str='1521',service_name:str=None,database:str=None) -> None:
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.conn = None
        self.database = database
        self.engine: sqlalchemy.engine.base.Engine = None

    def create_dbengine(self) -> sqlalchemy.engine.base.Engine:
        terminal.print_debug(txt=f'Inicio funcao dbconnect.create_dbengine():')
        connection_url = None
        if (self.database=='oracle'):
            connection_url = f'oracle+oracledb://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}'
        elif (self.database=='postgresql'):
           connection_url = 'postgresql+psycopg2://postgres:7LTZ3iTnM41WM*@localhost/postgres' 
        terminal.print_debug(f'String de conexao: {connection_url}')
        self.engine = sqlalchemy.create_engine(connection_url)
        terminal.print_debug(f"Retorno de dbconnect.create_engine(): {self.engine}, type: {type(self.engine)}")
        return self.engine

    def connect(self) -> sqlalchemy.engine.base.Engine:
        self.create_dbengine()
        return self.engine
    
    def execute_sql(self,final_sql_query=None,parameters=None):
        result = None
        with self.engine.connect() as connection:
            result = connection.execute(text(final_sql_query),parameters=parameters)
        return result
    
    def get_sql_result_as_data_frame(self,final_sql_query=None,parameters=None):
        self.connect()
        return pd.read_sql( final_sql_query, con=self.engine)
    
    def engine(self):
        return self.engine
    
    def commit(self):
        pass

    def close(self):
        pass