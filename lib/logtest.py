import logging
import logging.handlers
import lib


class NZBHandler(logging.Handler):
    def __init__(self, arg0, pwdb):
        logging.Handler.__init__(self)
        self.nzbname = arg0
        self.pwdb = pwdb

    def emit(self, record):
        # record.message is the log message
        self.pwdb.db_msg_insert(self.nzbname, record.message, record.levelname)


logger = logging.getLogger("ginzibix")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
logger.addHandler(fh)


pwdb = lib.PWDB(logger)
logging.handlers.NZBHandler = NZBHandler
nzbhandler = logging.handlers.NZBHandler("Der Gloeckner.nzb", pwdb)
logger.addHandler(nzbhandler)
nzbhandler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.info("servas " + str(h))

# logger.removeHandler(nzbhandler)

# logger.info("servas")
