import sys
import os
import signal
import time

lpref = __name__ + " - "


# captures SIGINT / SIGTERM and closes down everything
class SigHandler():

    def __init__(self, threads, pwdb, mpp_work_queue, mpp_pid, logger):
        self.logger = logger
        self.servers = None
        self.threads = threads
        self.mpp_work_queue = mpp_work_queue
        self.pwdb = pwdb
        self.signal = False
        self.mpp_renamer = None
        self.mpp_pid = mpp_pid

    def shutdown(self):
        f = open('/dev/null', 'w')
        sys.stdout = f
        for key, item in self.mpp_pid.items():
            self.logger.debug(lpref + "MPP " + key + ": " + str(item))
        # stop unrarer
        if self.mpp_pid["unrarer"]:
            self.logger.warning(lpref + "signalhandler: terminating unrarer")
            try:
                os.kill(self.mpp_pid["unrarer"], signal.SIGKILL)
            except Exception as e:
                self.logger.debug(lpref + str(e))
        # stop rar_verifier
        if self.mpp_pid["verifier"]:
            self.logger.warning(lpref + "signalhandler: terminating rar_verifier")
            try:
                os.kill(self.mpp_pid["verifier"], signal.SIGKILL)
            except Exception as e:
                self.logger.debug(lpref + str(e))
        # stop mpp_renamer
        if self.mpp_pid["renamer"]:
            self.logger.warning(lpref + "signalhandler: terminating renamer")
            try:
                os.kill(self.mpp_pid["renamer"], signal.SIGKILL)
            except Exception as e:
                self.logger.debug(lpref + str(e))
        # stop nzbparser
        if self.mpp_pid["nzbparser"]:
            self.logger.warning(lpref + "signalhandler: terminating nzb_parser")
            try:
                os.kill(self.mpp_pid["nzbparser"], signal.SIGKILL)
            except Exception as e:
                self.logger.debug(lpref + str(e))
        # stop article decoder
        if self.mpp_pid["decoder"]:
            self.logger.warning(lpref + "signalhandler: terminating article_decoder")
            self.mpp_work_queue.put(None)
            time.sleep(1)
            try:
                os.kill(self.mpp_pid["decoder"], signal.SIGKILL)
            except Exception as e:
                self.logger.debug(lpref + str(e))
        # stop pwdb
        self.logger.warning(lpref + "signalhandler: closing pewee.db")
        self.pwdb.db_close()
        # threads + servers
        self.logger.warning(lpref + "signalhandler: stopping download threads")
        for t, _ in self.threads:
            t.stop()
            t.join()
        if self.servers:
            self.logger.warning(lpref + "signalhandler: closing all server connections")
            self.servers.close_all_connections()
        self.logger.warning(lpref + "signalhandler: exiting")
        sys.exit()

    def signalhandler(self, signal, frame):
        self.shutdown()
