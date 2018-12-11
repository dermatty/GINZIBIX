from threading import Thread
import threading
import time
import psutil

# articlequeue: articles to download
# --> resultqueue: downloaded articles
# --> mpworkqueue:  
# monitors state of ginzibix subprocesses - article_decoder atm
class Monitor(Thread):
    def __init__(self, mpp, articlequeue, resultqueue, mpworkqueue, logger):
        Thread.__init__(self)
        self.daemon = True
        self.logger = logger
        self.lock = threading.Lock()
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.mpworkqueue = mpworkqueue
        self.mpp = mpp
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            lenartqueue = len(list(self.articlequeue.queue))
            lenresqueue = len(list(self.resultqueue.queue))
            lenworkqueue = self.mpworkqueue.qsize()
            try:
                decoder_is_alive = self.mpp["decoder"].is_alive()
            except Exception as e:
                decoder_is_alive = False
            try:
                unrarer_is_alive = self.mpp["unrarer"].is_alive()
            except Exception as e:
                unrarer_is_alive = False
            try:
                verifier_is_alive = self.mpp["verifier"].is_alive()
            except Exception as e:
                verifier_is_alive = False
            try:
                renamer_is_alive = self.mpp["renamer"].is_alive()
            except Exception as e:
                renamer_is_alive = False
            try:
                nzbparser_is_alive = self.mpp["nzbparser"].is_alive()
            except Exception as e:
                nzbparser_is_alive = False

            print(time.time())
            print("Decoder: ", decoder_is_alive, " Unrarer: ", unrarer_is_alive, " Verifier: ", verifier_is_alive, " Renamer: ", renamer_is_alive,
                  "NZB_Parser: ", nzbparser_is_alive)
            print("ArtQueue: ", lenartqueue,  " -> ResQueue: ", lenresqueue, " -> WorkQueue ", lenworkqueue)
            print("*" * 40)
            time.sleep(0.1)
