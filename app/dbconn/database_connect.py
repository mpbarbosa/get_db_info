from typing import Union
import oracledb
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import terminal


class DBConnData:
    def __init__(self) -> None:
        self.user: str = ""
        self.password: str = ""
        self.host: str = ""
        self.port: str = ""
        self.service_name: str = ""
        self.conn_method: str = "oracledb"  # or 'sqlalchemy'
        self.conn: Union[oracledb.Connection, psycopg2.extensions.connection, None] = (
            None
        )
        self.cur: Union[oracledb.Cursor, psycopg2.extensions.cursor, None] = None
        self.engine: Union[
            oracledb.Connection, psycopg2.extensions.connection, None
        ] = None

    def setuser(self, user: str):
        self.user = user
        return self

    def setpassword(self, password: str):
        self.password = password
        return self

    def sethost(self, host: str):
        self.host = host
        return self

    def setport(self, port: str):
        self.port = port

    def setservice_name(self, service_name: str):
        self.service_name = service_name
        return self

    def setconn_method(self, conn_method: str):
        self.conn_method = conn_method
        return self

    def setconn(self, conn: Union[oracledb.Connection, psycopg2.extensions.connection]):
        self.conn = conn
        return self

    def setcur(self, cur: Union[oracledb.Cursor, psycopg2.extensions.cursor]):
        self.cur = cur
        return self

    def setengine(
        self, engine: Union[oracledb.Connection, psycopg2.extensions.connection]
    ):
        self.engine = engine
        return self

    def get_dsn(self):
        return f"{self.host}:{self.port}/{self.service_name}"


class DatabaseConn:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: str = "1521",
        service_name: str = None,
        conn_method=None,
    ) -> None:
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.conn_method = conn_method
        self.conn = None
        self.cur = None
        self.engine = None

    def execute_sql(
        self, final_sql_query: str = None, parameters: Union[list, None] = None
    ):
        return self._execute(final_sql_query=final_sql_query, parameters=parameters)

    def get_sql_result_as_data_frame(
        self, final_sql_query: str = None, parameters: Union[list, None] = None
    ):
        terminal.print_debug(
            txt=f"Inicio da funcao OracleConn.get_sql_result_as_data_frame(final_sql_query={final_sql_query},parameters={parameters}"
        )
        self.connect()
        exec_result = self.execute_sql(
            final_sql_query=final_sql_query, parameters=parameters
        )
        terminal.print_debug(txt=f"exec_result={exec_result}")
        result = self.dbcursor().fetchall()
        return pd.DataFrame(
            result, columns=[col[0] for col in self.dbcursor().description]
        )

    def fetchone(
        self, final_sql_query: str = None, parameters: Union[list, None] = None
    ):
        terminal.print_debug(
            txt=f"Inicio da funcao DatabaseConn.get_sql_result_as_data_frame(final_sql_query={final_sql_query},parameters={parameters}"
        )
        self.connect()
        exec_result = self.execute_sql(
            final_sql_query=final_sql_query, parameters=parameters
        )
        terminal.print_debug(txt=f"exec_result={exec_result}")
        return self.dbcursor().fetchone()

    def dbcursor(self):
        if self.cur == None:
            self.cur = self.conn.cursor()
        return self.cur


class OracleConn(DatabaseConn):
    def _connect(self):
        dsn = self.get_dsn()
        terminal.print_debug(f"Connecting Oracle Database: {dsn} ...")
        self.conn = oracledb.connect(user=self.user, password=self.password, dsn=dsn)
        terminal.print_monitor(f"Connected to Oracle Database: {self.conn}!")
        return self.conn

    def _init_oracle_client(self):
        oracledb.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_21_13")

    def get_conn_url(self):
        return f"oracle+oracledb://{self.user}:{self.password}@{self.host}/?service_name={self.service_name}"

    def connect(self):
        self.conn = None
        terminal.print_debug(f"Metodo de conexao Oracle: {self.conn_method}")
        if self.conn_method == "oracledb":
            terminal.print_debug(
                f"Comecando criacao da conexao Oracle com metodo: {self.conn_method}"
            )
            try:
                self.conn = self._connect()
            except oracledb.Error as error:
                error_msg = str(error)
                terminal.print_error(
                    f"Error connecting to Oracle:" + error_msg, log_lvl=0
                )
                if "DPY-3015" in error_msg:
                    terminal.print_monitor(">>>Trocando para Thick mode")
                    self._init_oracle_client()
                    self._connect()
                elif "DPY-4011" in error_msg:
                    terminal.print_error(
                        "Nao foi possivel conectar com o banco de dados oracle: {dsn}"
                    )
                    terminal.print_warning("Saindo...")
                    raise error
                elif "DPY-6005" in error_msg:
                    # Error connecting to Oracle:DPY-6005: cannot connect to database (CONNECTION_ID=6+OfikKEUv/u+4EZSXgG5g==).
                    terminal.print_error(
                        f"Nao foi possivel conectar com o banco de dados oracle: {self.get_dsn()}"
                    )
                    terminal.print_warning("Saindo...")
                    raise error
        elif self.conn_method == "sqlalchemy":
            terminal.print_debug(f"Connecting Oracle Database using sqlalchemy: ...")
            connection_url = self.get_get_conn_url()
            self.engine = create_engine(connection_url)
            try:
                self.conn = self.engine.connect()
            except oracledb.Error as error:
                error_msg = str(error)
                terminal.print_error(
                    f"Error connecting to Oracle:" + error_msg, log_lvl=0
                )
                if "DPY-3015" in error_msg:
                    terminal.print_monitor(">>>Trocando para Thick mode")
                    oracledb.init_oracle_client(
                        lib_dir=r"C:\Oracle\instantclient_21_13"
                    )
                    self.conn = db.connect()

            terminal.print_monitor("Connected to Oracle Database!")
        terminal.print_debug(
            f"Retornando conexao Oracle: {self.conn}, type: {type(self.conn)}"
        )
        return self.conn

    def ping(self):
        return self.conn.ping()

    def close(self):
        return self.conn.close()

    def _execute(self, final_sql_query: str = None, parameters=None):
        return self.dbcursor().execute(final_sql_query, parameters=parameters)


class PostgresqlConn(DatabaseConn):
    def _connect(self):
        dsn = self.get_dsn()
        terminal.print_debug(f"Connecting Postgresql Database: {dsn} ...")
        self.conn = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.service_name,
        )
        terminal.print_monitor(
            f"Connected to Postgresql Database: {self.conn}, type: {type(self.conn)}!"
        )
        return self.conn

    def connect(self):
        self.conn = None
        terminal.print_debug(f"Metodo de conexao Postgresql: {self.conn_method}")
        if self.conn_method == "psycopg2":
            terminal.print_debug(
                f"Comecando criacao da conexao Postgresql com metodo: {self.conn_method}"
            )
            try:
                self.conn = self._connect()
            except psycopg2.Error as error:
                error_msg = str(error)
                terminal.print_error(
                    f"Error connecting to Oracle:" + error_msg, log_lvl=0
                )
        elif self.conn_method == "sqlalchemy":
            terminal.print_debug(
                f"Connecting PostgreSQL Database using sqlalchemy: ..."
            )
            connection_url = self.get_get_conn_url()
            self.engine = create_engine(connection_url)
            self.conn = self.engine.connect()

            terminal.print_monitor("Connected to PostgreSQL Database!")
        terminal.print_debug(
            f"Retornando conexao PostgreSQL: {self.conn}, type: {type(self.conn)}"
        )
        return self.conn

    def _execute(self, final_sql_query: str = None, parameters=None):
        return self.dbcursor().execute(final_sql_query, vars=parameters)
