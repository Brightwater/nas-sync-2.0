import logging
import psycopg
from utility.psycopgUtil import getConn

class PgHandler(logging.Handler):
    def __init__(self):
        super().__init__()

        self.conn = getConn()

    def emit(self, record):
        if self.conn == None or self.conn.closed:
            self.conn = getConn()
            
        # Log the message to the database
        with self.conn.cursor() as cur:
            msg = record.msg
            if record.exc_info:
                msg = msg + " Exception: " + str(record.exc_info)
            cur.execute(f"""INSERT INTO logs (app_module, level, ts, message) 
                                    VALUES ('{record.name}', '{record.levelname}', NOW(), 
                                    %s)""", (msg,))
            self.conn.commit()
            

class CustomLog():
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)

        self.logger.addHandler(ch)
        pg = PgHandler()
        pg.setLevel(logging.DEBUG)
        self.logger.addHandler(pg)
     
    def info(self, message):
        self.logger.info(message)

    def warn(self, message):
        self.logger.warning(message)

    def debug(self, message):
        self.logger.debug(message)

    def error(self, message, exception=None):
        if exception:
            self.logger.error(message, exc_info=exception)
        else:
            self.logger.error(message)

