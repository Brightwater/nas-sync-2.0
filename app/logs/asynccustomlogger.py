import asyncio
import logging
from utility.database import database


#CUSTOM_CONSOLE_LOG = False

class AsyncPgHandler(logging.Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        # Log the message to the database
        asyncio.create_task(self.insertLog(record))
        
    async def insertLog(self, record):
        msg = str(record.msg)
        if record.exc_info:
            msg = msg + " Exception: " + str(record.exc_info)
        query = f"""INSERT INTO logs (app_module, level, ts, message) 
                                VALUES ('{record.name}', '{record.levelname}', NOW(), 
                                :msg)"""
        values = {"msg": msg}
        await database.execute(query=query, values=values)
        

            
class AsyncStreamHandler(logging.StreamHandler):
    def emit(self, record):
        message = self.format(record)
        stream = self.stream
        try:
            stream.write(message+"\n")
            self.flush()
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)
            
class Asynclog():
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        asyncHandler = AsyncStreamHandler()
        asyncHandler.setLevel(logging.DEBUG)
        asyncHandler.setFormatter(formatter)

        self.logger.addHandler(asyncHandler)

        asyncPgHandler = AsyncPgHandler()
        asyncPgHandler.setLevel(logging.DEBUG)
        self.logger.addHandler(asyncPgHandler)

    def info(self, message):
        self.logger.info(message)
        
    def apiinfo(self, message):
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

class AsyncApilog():
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        asyncPgHandler = AsyncPgHandler()
        asyncPgHandler.setLevel(logging.DEBUG)
        self.logger.addHandler(asyncPgHandler)

    def info(self, message):
        self.logger.info(message)
    