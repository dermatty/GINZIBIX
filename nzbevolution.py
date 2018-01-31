import threading
from threading import Thread
import time
# import queue
from random import randint
import sys
import os
import queue
from os.path import expanduser
import configparser
import signal
import glob
import xml.etree.ElementTree as ET
import nntplib
import ssl
import posix_ipc
import yenc

USERHOME = expanduser("~")
MAIN_DIR = USERHOME + "/.nzbbussi/"
CONFIG_DIR = MAIN_DIR + "config/"
NZB_DIR = MAIN_DIR + "nzb/"
COMPLETE_DIR = MAIN_DIR + "complete/"
INCOMPLETE_DIR = MAIN_DIR + "incomplete/"
LOGS_DIR = MAIN_DIR + "logs/"

LOCK = threading.Lock()
SEMAPHORE = posix_ipc.Semaphore("news_sema", posix_ipc.O_CREAT)
SEMAPHORE.release()


# ---- Procedures ----


def siginthandler(signum, frame):
    global SERVERS
    SERVERS.quit()
    sys.exit()


def ParseNZB(nzbroot):
    filedic = {}
    for r in nzbroot:
        headers = r.attrib
        try:
            # poster = headers["poster"]
            date = headers["date"]
            age = int((time.time() - float(date))/(24 * 3600))
            subject = headers["subject"]
            # print(subject)
            hn_list = subject.split('"')
            hn = hn_list[1]
            # an = hn_list[0]
            filetype = hn.split(".")[-1]
            if filetype.lower() == "par2":
                if hn.split(".")[-2][:3] == "vol":
                    filetype = "PAR2"
            # if PAR2FILE is None and hn[-5:].lower() == ".par2":
            #     PAR2FILE = hn
        except Exception as e:
            continue
        for s in r:
            filelist = []
            segfound = True
            for i, r0 in enumerate(s):
                if r0.tag[-5:] == "group":
                    segfound = False
                    continue
                nr0 = r0.attrib["number"]
                filename = "<" + r0.text + ">"
                filelist.append((filename, int(nr0)))
            i -= 1
            if segfound:
                filelist.insert(0, (age, filetype, int(nr0)))
                filedic[hn] = filelist
    return filedic


def download(article, nntpob):
    r = randint(0, 99)
    # return True, "info!"
    if r <= 70:
        return True, "info!"
    else:
        return False, False


# ---- Classes ----


class Servers():

    def __init__(self, cfg):
        self.cfg = cfg
        # server_config = [(server_name, server_url, user, password, port, usessl, level, connections, retention)]
        self.server_config = self.get_server_config(self.cfg)
        # all_connections = [(conn#, server_name)]
        self.all_connections = self.get_all_connections()
        # level_servers0 = {"0": ["EWEKA", "BULK"], "1": ["TWEAK"], "2": ["NEWS", "BALD"]}
        self.level_servers = self.get_level_servers()

    def __bool__(self):
        if not self.server_config:
            return False
        return True

    def get_all_connections(self):
        conn = []
        for s_name, _, _, _, _, _, _, s_connections, _ in self.server_config:
            for c in range(s_connections):
                conn.append((s_name, c + 1))
        return conn

    def get_level_servers(self):
        s_tuples = []
        for s_name, _, _, _, _, _, s_level, _, _ in self.server_config:
            s_tuples.append((s_name, s_level))
        sorted_s_tuples = sorted(s_tuples, key=lambda server: server[1])
        ls = {}
        for s_name, s_level in sorted_s_tuples:
            if str(s_level) not in ls:
                ls[str(s_level)] = []
            ls[str(s_level)].append(s_name)
        return ls

        '''def quit(self):
        for s in self.server_threadlist:
            server_name, connlist, _, _ = s
            for i, (server0, _) in enumerate(connlist):
                if not server0:
                    continue
                try:
                    server0.quit()
                    print("Connection to server " + server_name + " / connection #" + str(i+1) + " closed!")
                except Exception as e:
                    print("Server quit error: " + str(e))'''

    '''def make_connection(self, server_idx, connection_idx):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        server_name, server, user, password, port, usessl, level, max_connections, retention, threads_active = self.server_config[server_idx]
        (server_name_t, connlist_t, retention_t, level_t) = self.server_threadlist[server_idx]
        try:
            if usessl:
                server0 = nntplib.NNTP_SSL(server, user=user, password=password, ssl_context=context, port=port, readermode=True)
            else:
                server0 = nntplib.NNTP(server, user=user, password=password, ssl_context=context, port=port, readermode=True)
            print("Established connection #" + str(connection_idx + 1) + "on server " + server_name)
            connlist_t[connection_idx] = (server0, False)
            self.server_threadlist[server_idx] = (server_name_t, connlist_t, retention_t, level_t)
            return server0
        except Exception as e:
            print("Cannot connect to server " + server_name)
        return False'''

    '''def connect(self):
        serverthreads = []
        unused = []
        for s in self.server_config:
            server_name, server, user, password, port, usessl, level, max_connections, retention, threads_active = s
            connlist = []
            for mc in range(max_connections):
                try:
                    connlist.append((False, False))
                except Exception as e:
                    pass
            if connlist:
                unused.append(server_name)
                serverthreads.append((server_name, connlist, retention, level))
            else:
                print("Cannot connect to server " + server_name + ", skipping ...")
        return serverthreads, unused'''

    def get_server_config(self, cfg):
        # get servers from config
        snr = 0
        sconf = []
        while True:
            try:
                snr += 1
                snrstr = "SERVER" + str(snr)
                server_name = cfg[snrstr]["SERVER_NAME"]
                server_url = cfg[snrstr]["SERVER_URL"]
                user = cfg[snrstr]["USER"]
                password = cfg[snrstr]["PASSWORD"]
                port = int(cfg[snrstr]["PORT"])
                usessl = True if cfg[snrstr]["SSL"].lower() == "yes" else False
                level = int(cfg[snrstr]["LEVEL"])
                connections = int(cfg[snrstr]["CONNECTIONS"])
            except Exception as e:
                snr -= 1
                break
            try:
                retention = int(cfg[snrstr]["RETENTION"])
                sconf.append((server_name, server_url, user, password, port, usessl, level, connections, retention))
            except Exception as e:
                sconf.append((server_name, server_url, user, password, port, usessl, level, connections, 999999))
        if not sconf:
            return None
        return sconf


class ConnectionWorker(Thread):
    def __init__(self, lock, connection, articlequeue):
        Thread.__init__(self)
        self.connection = connection
        self.articlequeue = articlequeue
        self.lock = lock
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        global resultarticles_dic
        name, nntpobj = self.connection
        idn = name + str(nntpobj)
        print(idn + " started!")
        while self.running:
            try:
                article = self.articlequeue.get_nowait()
                articlenr, remaining_servers = article
                self.articlequeue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print("Error in article queue get: " + str(e))
            if name not in remaining_servers:
                self.articlequeue.put(article)
            else:
                print("Downloading on server " + idn + ": + for article #" + str(articlenr), remaining_servers)
                res, info = download(article, nntpobj)
                if res:
                    print("Download success on server " + idn + ": for article #" + str(articlenr), remaining_servers)
                    with self.lock:
                        resultarticles_dic[str(articlenr)] = info
                else:
                    article = (articlenr, [x for x in remaining_servers if x != name])
                    if not article[1]:
                        print(">>>> Download finally failed on server " + idn + ": for article #" + str(articlenr), article[1])
                        with self.lock:
                            resultarticles_dic[str(articlenr)] = None
                    else:
                        self.articlequeue.put(article)
                        print(">>>> Download failed on server " + idn + ": for article #" + str(articlenr), ", requeuing on servers:",
                              article[1])
        print(idn + " exited!")


def ArticleProducer(articles, articlequeue):
    for article in articles:
        articlequeue.put(article)


# main
if __name__ == '__main__':

    CFG = configparser.ConfigParser()
    CFG.read(CONFIG_DIR + "/nzbbussi.config")

    # get servers
    SERVERS = Servers(CFG)
    if not SERVERS:
        print("At least one server has to be provided, exiting!")
        sys.exit()

    signal.signal(signal.SIGINT, siginthandler)
    signal.signal(signal.SIGTERM, siginthandler)

    # get nzbs -> atm only pls only 1 nzb in nzb dir!
    os.chdir(NZB_DIR)
    print("Getting NZB files from " + NZB_DIR)
    for NZB in glob.glob("*.nzb"):
        pass
    try:
        tree = ET.parse(NZB)
        print("Downloading " + NZB)
    except Exception as e:
        print(str(e) + ": please provide at least 1 NZB file, exiting!")
        sys.exit()
    nzb_root = tree.getroot()
    os.chdir(MAIN_DIR)

    filedic = ParseNZB(nzb_root)

    nr_articles = 134
    storedic = {}
    articlequeue = queue.Queue()
    articles0 = [str(i+1) for i in range(nr_articles)]
    resultarticles_dic = {key: None for key in articles0}

    all_connections = SERVERS.all_connections
    level_servers0 = SERVERS.level_servers

    t0 = time.time()
    for level, serverlist in level_servers0.items():
        level_servers = serverlist
        articles = [(key, level_servers) for key, item in resultarticles_dic.items() if not item]
        if not articles:
            print("All articles downloaded")
            break
        print("####", articles)
        level_connections = [(name, connection) for name, connection in all_connections if name in level_servers]
        if not level_connections:
            continue
        # Produce
        ArticleProducer(articles, articlequeue)
        # consumer
        threads = []
        for c in level_connections:
            t = ConnectionWorker(LOCK, c, articlequeue)
            threads.append(t)
            t.start()

        articlequeue.join()

        for t in threads:
            t.stop()
            t.join()

        print("Download failed:", [(key, item) for key, item in resultarticles_dic.items() if not item])
        l0 = len([item for key, item in resultarticles_dic.items()])
        l1 = len([item for key, item in resultarticles_dic.items() if item])
        print("Complete  Articles after level", level, ": " + str(l1) + " out of " + str(l0))
        print("-" * 80)
    print(time.time() - t0)
