import logging
import logging.handlers
import lib
import time


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

nzbname = "Der Gloeckner.nzb"

pwdb = lib.PWDB(logger)
logging.handlers.NZBHandler = NZBHandler
nzbhandler = logging.handlers.NZBHandler(nzbname, pwdb)
logger.addHandler(nzbhandler)
nzbhandler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

t0 = time.time()
for n in range(0, 3):
    logger.info("servas ")
print(time.time() - t0)

msg0, ll = pwdb.db_msg_get(nzbname)
print(msg0[ll].nzbname, msg0[ll].timestamp, msg0[ll].message)

for m in msg0:
    print(m.nzbname, m.timestamp, m.message)

pwdb.db_msg_removeall(nzbname)
logger.removeHandler(nzbhandler)

logger.info("servas00")
