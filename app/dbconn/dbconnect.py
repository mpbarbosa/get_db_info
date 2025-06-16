import sqlite3
import mysql.connector
import psycopg2
from database_connect import DatabaseConn, OracleConn
from .sqlalchemy_connection import SqlalchemyConn
from database_connect import PostgresqlConn
from config import load_config
from dbconfig import DbConfig

from abc import ABC, abstractmethod


class DatabaseConnection(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass


class MySQLConnection(DatabaseConnection):
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.connection = None

    def connect(self):
        print("Connecting to MySQL database...")
        self.connection = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
        )

    def close(self):
        if self.connection:
            self.connection.close()
            print("MySQL connection closed.")

class PostgreSQLConnection(DatabaseConnection):
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.connection = None

    def connect(self):
        print("Connecting to PostgreSQL database...")
        self.connection = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
       )

    def close(self):
        if self.connection:
        self.connection.close()
        print("PostgreSQL connection closed.")

class SQLiteConnection(DatabaseConnection):
    def __init__(self, database):
        self.database = database
        self.connection = None

    def connect(self):
        print("Connecting to SQLite database...")
        self.connection = sqlite3.connect(self.database)

    def close(self):
        if self.connection:
            self.connection.close()
            print("SQLite connection closed.")


def open_postgresql_dbconnection(dbconfig: DbConfig = None, conn_method: str = None):
    config = dbconfig.config
    user = config["user"]
    password = config["password"]
    host = config["host"]
    port = config["port"]
    service_name = config.get("service", None)
    conn = None
    if conn_method == "psycopg2":
        config = load_config(filename="database.ini", section="postgresql")
        try:
            conn = PostgresqlConn(
                user=user,
                password=password,
                host=host,
                port=port,
                service_name=service_name,
                conn_method=conn_method,
            )
        except (psycopg2.DatabaseError, Exception) as error:
            raise error
    elif conn_method == "sqlalchemy":
        config = dbconfig.config
        user = config["user"]
        password = config["password"]
        host = config["host"]
        service_name = config.get("service", None)
        port = config["port"]
        conn = None
        conn = SqlalchemyConn(
            user=user, password=password, host=host, database="postgresql"
        )
    return conn


def open_oracle_dbconnection(
    dbconfig: DbConfig = None, conn_method: str = None
) -> DatabaseConn:
    terminal.print_debug(
        txt=f"Inicio funcao dbconnect.open_oracle_dbconnection(dbconfig: {dbconfig}"
    )
    config = dbconfig.config
    user = config["user"]
    password = config["password"]
    host = config["host"]
    service_name = config.get("service", None)
    port = config["port"]
    conn: DatabaseConn = None
    if conn_method == "oracledb":
        conn = OracleConn(
            user=user,
            password=password,
            host=host,
            port=port,
            service_name=service_name,
            conn_method=conn_method,
        )
    elif conn_method == "sqlalchemy":
        conn = SqlalchemyConn(
            user=user,
            password=password,
            host=host,
            port=port,
            service_name=service_name,
            database="oracle",
        )
    terminal.print_debug(txt=f"Conexao Oracle criada: {conn}")
    conn.connect()
    return conn


def open_dbconnection(dbconfig: DbConfig = None, database=None, conn_method=None):
    conn = None
    if database == "oracle":
        conn = open_oracle_dbconnection(dbconfig=dbconfig, conn_method=conn_method)
    elif database == "postgresql":
        conn = open_postgresql_dbconnection(dbconfig=dbconfig, conn_method=conn_method)
    else:
        print("Database invalido")
        exit()
    return conn


def close_dbconnection(conn):
    if conn:
        conn.close()
        terminal.print_monitor("Database connection closed.")
