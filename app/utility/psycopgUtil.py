import psycopg
from psycopg.rows import dict_row

def getConn():
    return psycopg.connect(user = "postgres",
                                    password = "",
                                    host = "127.0.0.1",
                                    port = "5432",
                                    dbname = "nassync",
                                    row_factory=dict_row)
