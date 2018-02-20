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
import multiprocessing as mp
from pympler import asizeof


USERHOME = expanduser("~")
MAIN_DIR = USERHOME + "/.nzbbussi/"
CONFIG_DIR = MAIN_DIR + "config/"
NZB_DIR = MAIN_DIR + "nzb/"
COMPLETE_DIR = MAIN_DIR + "complete/"
INCOMPLETE_DIR = MAIN_DIR + "incomplete/"
LOGS_DIR = MAIN_DIR + "logs/"

SEMAPHORE = posix_ipc.Semaphore("news_sema", posix_ipc.O_CREAT)
SEMAPHORE.release()


# ---- Procedures ----


def decode_articles(mp_work_queue, mp_result_queue):
    while True:
        try:
            res0 = mp_work_queue.get()
        except KeyboardInterrupt:
            return
        if not res0:
            print("Exiting decoder process!")
            break
        infolist, complete_dir, filename = res0
        bytes0 = bytearray()
        bytesfinal = bytearray()
        pcrc32 = ""
        status = 0   # 1: ok, 0: wrong yenc structure, -1: no crc32, -2: crc32 checksum error, -3: decoding error
        statusmsg = "ok"
        for info in infolist:
            headerfound = 0
            endfound = 0
            partfound = 0
            try:
                assert(info.lines)
            except Exception as e:
                status = -5
                statusmsg = "no_info.list"
                print("--> " + filename + " / " + str(e))
                continue
            for inf in info.lines:
                try:
                    inf0 = inf.decode()
                    if inf0 == "":
                            continue
                    if inf0[:7] == "=ybegin":
                        headerfound += 1
                        continue
                    if inf0[:5] == "=yend":
                        pcrc32 = inf0.split("pcrc32=")[1]
                        endfound += 1
                        continue
                    if inf0[:6] == "=ypart":
                        partfound += 1
                        continue
                except Exception as e:
                    status = -3
                    statusmsg = "no_decode_error"
                bytes0.extend(inf)
            if headerfound != 1 or endfound != 1 or partfound > 1:
                print("Wrong yenc structure detected")
                statusmsg = "yenc_structure_error"
                status = 0
            if pcrc32 == "":
                statusmsg = "no_pcrc32_error"
                status = -1
            _, crc32, decoded = yenc.decode(bytes0)
            if crc32.strip("0") != pcrc32.strip("0"):
                print("CRC32 checksum error: " + crc32 + " / " + pcrc32)
                statusmsg = "crc32checksum_error: " + crc32 + " / " + pcrc32
                status = -2
            bytesfinal.extend(decoded)
            bytes0 = bytearray()
        # return bytesfinal, status  # status = 0 -> ok, = -1 -> repair (par2) needed
        try:
            with open(complete_dir + filename, "wb") as f0:
                f0.write(bytesfinal)
                f0.flush()
                f0.close()
        except Exception as e:
            statusmsg = "file_error"
            status = -4
        mp_result_queue.put((filename, status, statusmsg))


def ParseNZB(nzbdir):
    cwd0 = os.getcwd()
    os.chdir(nzbdir)
    print("Getting NZB files from " + nzbdir)
    for nzb in glob.glob("*.nzb"):
        pass
    try:
        tree = ET.parse(nzb)
        print("Downloading " + nzb)
    except Exception as e:
        print(str(e) + ": please provide at least 1 NZB file!")
        return None
    nzbroot = tree.getroot()
    os.chdir(cwd0)
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
    if r <= 95:
        return True, "info!"
    else:
        return False, False


# ---- Classes ----


class SigHandler():
    def __init__(self, servers, threads, mp_work_queue):
        self.servers = servers
        self.threads = threads
        self.mp_work_queue = mp_work_queue
        self.signal = False

    def handler2(self, signal, frame):
        return

    def signalhandler(self, signal, frame):
        self.signal = True
        print("Got Ctrl-C")
        self.mp_work_queue.put(None)
        time.sleep(1)
        for t in self.threads:
            t.stop()
            t.join()
        self.servers.close_all_connections()
        sys.exit()


class Servers():

    def __init__(self, cfg):
        self.cfg = cfg
        # server_config = [(server_name, server_url, user, password, port, usessl, level, connections, retention)]
        self.server_config = self.get_server_config(self.cfg)
        # all_connections = [(server_name, conn#, retention, nntp_obj)]
        self.all_connections = self.get_all_connections()
        # level_servers0 = {"0": ["EWEKA", "BULK"], "1": ["TWEAK"], "2": ["NEWS", "BALD"]}
        self.level_servers = self.get_level_servers()

    def __bool__(self):
        if not self.server_config:
            return False
        return True

    def get_single_server_config(self, server_name0):
        for server_name, server_url, user, password, port, usessl, level, connections, retention in self.server_config:
            if server_name == server_name0:
                return server_name, server_url, user, password, port, usessl, level, connections, retention
        return None

    def get_all_connections(self):
        conn = []
        for s_name, _, _, _, _, _, _, s_connections, s_retention in self.server_config:
            for c in range(s_connections):
                conn.append((s_name, c + 1, s_retention, None))
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

    def open_connection(self, server_name0, conn_nr):
        result = None
        for idx, (sn, cn, rt, nobj) in enumerate(self.all_connections):
            if sn == server_name0 and cn == conn_nr:
                if nobj:
                    return nobj
                else:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                    sc = self.get_single_server_config(server_name0)
                    if sc:
                        server_name, server_url, user, password, port, usessl, level, connections, retention = self.get_single_server_config(server_name0)
                        try:
                            if usessl:
                                nntpobj = nntplib.NNTP_SSL(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True)
                            else:
                                nntpobj = nntplib.NNTP(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True)
                            print("Opened Connection #" + str(conn_nr) + " on server " + server_name0)
                            result = nntpobj
                            self.all_connections[idx] = (sn, cn, rt, nntpobj)
                            break
                        except Exception as e:
                            print("Server " + server_name0 + " connect error: " + str(e))
                            self.all_connections[idx] = (sn, cn, rt, None)
                            break
                    else:
                        print("Cannot get server config for server: " + server_name0)
                        self.all_connections[idx] = (sn, cn, rt, None)
                        break
        return result

    def close_all_connections(self):
        for (sn, cn, _, nobj) in self.all_connections:
            if nobj:
                try:
                    nobj.quit()
                    print("Closed connection #" + str(cn) + " on " + sn)
                except Exception as e:
                    print("Cannot quit server " + sn + ": " + str(e))

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
    def __init__(self, lock, connection, articlequeue, resultqueue, servers):
        Thread.__init__(self)
        # self.daemon = True
        self.connection = connection
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.lock = lock
        self.servers = servers
        self.nntpobj = None
        self.running = True

    def stop(self):
        self.running = False

    def download_article(self, article_name, article_age):
        sn, _ = self.connection
        server_name, server_url, user, password, port, usessl, level, connections, retention = self.servers.get_single_server_config(sn)
        if retention < article_age * 0.95:
            return False, None
        try:
            # resp_h, info_h = self.nntpobj.head(article_name)
            resp, info = self.nntpobj.body(article_name)
            if resp[:3] != "222":
            # if resp_h[:3] != "221" or resp[:3] != "222":
                raise("No Succes answer from server!")
            else:
                resp = True
        except Exception as e:
            resp = False
            info = None
            print("Article download error: " + str(e))
        return resp, info

    def run(self):
        name, conn_nr = self.connection
        idn = name + " #" + str(conn_nr)
        if not self.nntpobj:
            self.nntpobj = self.servers.open_connection(name, conn_nr)
            if not self.nntpobj:
                print("Could not connect to server " + idn + ", exiting thread")
                # self.stop()   # todo: ordentlicher stop!!!
                return
        else:
            print(idn + " starting !")
        while True and self.running:    # self.running:
            self.lock.acquire()
            artlist = list(self.articlequeue.queue)
            try:
                test_article = artlist[-1]
            except IndexError:
                self.lock.release()
                time.sleep(0.1)
                continue
            if not test_article:
                article = self.articlequeue.get()
                self.articlequeue.task_done()
                self.lock.release()
                print(idn + ": got poison pill!")
                break
            _, _, _, _, _, _, remaining_servers = test_article
            # no servers left
            # articlequeue = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
            if not remaining_servers:
                article = self.articlequeue.get()
                self.lock.release()
                self.resultqueue.put(article + (None,))
                '''a_fn, a_a, a_ft, a_no, a_nr, a_n, a_l = self.articlequeue.get()
                self.lock.release()
                resultqueue.put((a_fn, a_a, a_ft, a_no, a_nr, a_n, a_l, None))'''
                continue
            if name not in remaining_servers[0]:
                self.lock.release()
                time.sleep(0.1)
                continue
            article = self.articlequeue.get()
            self.lock.release()
            filename, age, filetype, nr_articles, art_nr, art_name, remaining_servers1 = article
            print("Downloading on server " + idn + ": + for article #" + str(art_nr), filename)
            res, info = self.download_article(art_name, age)
            # res, info = download(article, self.nntpobj)
            if res:
                print("Download success on server " + idn + ": for article #" + str(art_nr), filename)
                self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, remaining_servers, info))
                self.articlequeue.task_done()
            else:
                # print("###", name, remaining_servers, remaining_servers1)
                next_servers = []
                for s in remaining_servers:
                    addserver = s[:]
                    try:
                        addserver.remove(name)
                    except Exception as e:
                        pass
                        # print(addserver, " / ", name)
                    if addserver:
                        next_servers.append(addserver)
                self.articlequeue.task_done()
                if not next_servers:
                    print("Download finally failed on server " + idn + ": for article #" + str(art_nr), next_servers)
                    self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, [], "failed"))
                else:
                    print("Download failed on server " + idn + ": for article #" + str(art_nr) + ", queueing: ", next_servers)
                    self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
        print(idn + " exited!")


class Downloader():
    def __init__(self, servers):
        self.servers = servers
        self.lock = threading.Lock()
        self.level_servers = self.servers.level_servers
        self.all_connections = self.servers.all_connections
        self.articlequeue = queue.LifoQueue()
        self.resultqueue = queue.Queue()
        self.mp_work_queue = mp.Queue()
        self.mp_result_queue = mp.Queue()
        self.threads = []

    def article_producer(self, articles, articlequeue):
        for article in articles:
            articlequeue.put(article)

    def make_allfilelist(self, filedic):
        allfilelist = []
        for idx, (filename, filelist) in enumerate(filedic.items()):
            for i, f in enumerate(filelist):
                if i == 0:
                    age, filetype, nr_articles = f
                    allfilelist.append([(filename, age, filetype, nr_articles)])
                else:
                    fn, nr = f
                    allok = True    # check for duplicate art. #
                    if len(allfilelist[idx]) > 2:
                        for i1, art in enumerate(allfilelist[idx]):
                            if i1 > 1:
                                nr1, fn1 = art
                                if nr1 == nr:
                                    allok = False
                                    break
                    if allok:
                        allfilelist[idx].append((nr, fn))
        return allfilelist

    def download_and_process(self, filedic):
        global COMPLETE_DIR

        allfileslist = self.make_allfilelist(filedic)

        # generate all articles and files
        files = {}
        for j, file_articles in enumerate(reversed(allfileslist)):
            # iterate over all articles in file
            filename, age, filetype, nr_articles = file_articles[0]
            level_servers = self.get_level_servers(age)
            files[filename] = (nr_articles, age, filetype, [None] * nr_articles, False, True)
            for i, art0 in enumerate(file_articles):
                if i == 0:
                    continue
                art_nr, art_name = art0
                q = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
                self.articlequeue.put(q)

        # start decoder thread
        mpp = mp.Process(target=decode_articles, args=(self.mp_work_queue, self.mp_result_queue, ))
        mpp.start()

        t0 = time.time()
        bytesdownloaded = 0

        # start all connection worker threads
        for sn, scon, _, _ in self.all_connections:
            t = ConnectionWorker(self.lock, (sn, scon), self.articlequeue, self.resultqueue, self.servers)
            self.threads.append(t)
            t.start()

        # register sigint/sigterm handlers
        self.sighandler = SigHandler(self.servers, self.threads, self.mp_work_queue)
        signal.signal(signal.SIGINT, self.sighandler.signalhandler)
        signal.signal(signal.SIGTERM, self.sighandler.signalhandler)

        while True and not self.sighandler.signal:
            # read resultqueue
            results = []
            while True:
                try:
                    resultarticle = self.resultqueue.get_nowait()
                    results.append(resultarticle)
                    self.resultqueue.task_done()
                except queue.Empty:
                    break
            # distribute results to files
            if results:
                for r in results:
                    filename, age, filetype, nr_articles, art_nr, art_name, remaining_servers, info = r
                    # print(">>>", asizeof.asizeof(info))
                    bytesdownloaded += sum(len(i) for i in info.lines)
                    (f_nr_articles, f_age, f_filetype, infolist, done, failed) = files[filename]
                    infolist0 = infolist[:]
                    infolist0[art_nr-1] = info
                    files[filename] = (f_nr_articles, f_age, f_filetype, infolist0, done, failed)
            # set completed files to "done" & decode
            for filename, (f_nr_articles, f_age, f_filetype, infolist0, done, failed) in files.items():
                # print(80 * "-")
                # print(filename, done, len([inf for inf in infolist0 if not inf]))
                if not done and len([inf for inf in infolist0 if inf]) == f_nr_articles:        # check for failed!! todo!!
                    if "failed" not in infolist:
                        failed0 = False
                    else:
                        print(filename + "failed!!")
                        failed0 = True
                    files[filename] = (f_nr_articles, f_age, f_filetype, infolist0, True, failed0)
                    print("All articles for " + filename + " downloaded, calling mp.decode ...")
                    self.mp_work_queue.put((infolist0, COMPLETE_DIR, filename))
            # start decoding/saving for done files
            alldone = True
            for filename, (f_nr_articles, f_age, f_filetype, infolist0, done, failed) in files.items():
                if not done:
                    alldone = False
            # if all are done: exit loop
            if alldone:
                break
            print("MBit/sec.: ", (bytesdownloaded / (time.time() - t0)) / (1024 * 1024) * 8)
            time.sleep(0.1)
        
        if self.sighandler.signal:
            time.sleep(1000)

        # clean up
        print("cleaning up ...")
        self.mp_work_queue.put(None)
        self.resultqueue.join()
        self.articlequeue.join()
        for t in self.threads:
            t.stop()
            t.join()
        self.servers.close_all_connections()

    def get_level_servers(self, retention):
        le_serv0 = []
        for level, serverlist in self.level_servers.items():
            level_servers = serverlist
            le_dic = {}
            for le in level_servers:
                _, _, _, _, _, _, _, _, age = self.servers.get_single_server_config(le)
                le_dic[le] = age
            les = [le for le in level_servers if le_dic[le] > retention * 0.9]
            le_serv0.append(les)
        return le_serv0


# main
if __name__ == '__main__':

    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_DIR + "/nzbbussi.config")

    # get servers
    servers = Servers(cfg)
    if not servers:
        print("At least one server has to be provided, exiting!")
        sys.exit()

    filedic = ParseNZB(NZB_DIR)

    dl = Downloader(servers)
    status = dl.download_and_process(filedic)
