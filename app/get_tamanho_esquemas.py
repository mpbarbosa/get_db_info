import oracledb

connection = oracledb.connect(user="system", password="mpb", dsn="localhost/XEPDB1")
connection.close()
