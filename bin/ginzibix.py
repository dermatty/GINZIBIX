#!/home/stephan/.virtualenvs/nntp/bin/python

import threading
from threading import Thread
import time
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
import yenc
import multiprocessing as mp
import logging
import logging.handlers
import psutil
import re
import par2lib
import hashlib
import rarfile
import subprocess
import pymongo

# ------------------- pymongo ---------------------

DB_URL = "mongodb://ubuntuvm1.iv.at:27017"
DB_NAME = "ginzibix_db"
MONGODB = pymongo.MongoClient(DB_URL)[DB_NAME]
# testinsert = {"testdata": ["1", "2"]}
# res00 = MONGODB["TEST"].insert_one(testinsert)

# -------------------- globals --------------------

userhome = expanduser("~")
maindir = userhome + "/.ginzibix/"
dirs = {
    "userhome": userhome,
    "main": maindir,
    "config": maindir + "config/",
    "nzb": maindir + "nzb/",
    "complete": maindir + "complete/",
    "incomplete": maindir + "incomplete/",
    "logs": maindir + "logs/"
}
_ftypes = ["etc", "rar", "sfv", "par2", "par2vol"]

# init logger
logger = logging.getLogger("ginzibix")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(dirs["logs"] + "ginzibix.log", mode="w")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


# -------------------- Procedures --------------------

def calc_file_md5hash(fn):
    hash_md5 = hashlib.md5()
    try:
        with open(fn, "rb") as f0:
            for chunk in iter(lambda: f0.read(4096), b""):
                hash_md5.update(chunk)
        md5 = hash_md5.digest()
    except Exception as e:
        logger.warning("MD5 calc error: " + str(e))
        md5 = -1
    return md5


def decode_articles(mp_work_queue0, mp_result_queue0, logger):
    bytes0 = bytearray()
    bytesfinal = bytearray()
    while True:
        try:
            res0 = mp_work_queue0.get()
        except KeyboardInterrupt:
            return
        if not res0:
            logger.info("Exiting decoder process!")
            break
        infolist, save_dir, filename, filetype = res0
        del bytes0
        bytesfinal = bytearray()
        status = 0   # 1: ok, 0: wrong yenc structure, -1: no crc32, -2: crc32 checksum error, -3: decoding error
        statusmsg = "ok"
        for info in infolist:
            headerok = False
            trailerok = False
            partfound = False
            trail_crc = None
            head_crc = None
            bytes0 = bytearray()
            partnr = 0
            artsize0 = 0
            for inf in info:
                try:
                    inf0 = inf.decode()
                    if inf0 == "":
                        continue
                    if inf0.startswith("=ybegin"):
                        try:
                            artname = re.search(r"name=(\S+)", inf0).group(1)
                            artsize = int(re.search(r"size=(\S+)", inf0).group(1))
                            artsize0 += artsize
                            m_obj = re.search(r"crc32=(\S+)", inf0)
                            if m_obj:
                                head_crc = m_obj.group(1)
                            headerok = True
                        except Exception as e:
                            logger.warning(str(e) + ": malformed =ybegin header in article " + artname)
                        continue
                    if inf0.startswith("=ypart"):
                        partfound = True
                        partnr += 1
                        continue
                    if inf0.startswith("=yend"):
                        try:
                            artsize = int(re.search(r"size=(\S+)", inf0).group(1))
                            m_obj = re.search(r"crc32=(\S+)", inf0)
                            if m_obj:
                                trail_crc = m_obj.group(1)
                            trailerok = True
                        except Exception as e:
                            logger.warning(str(e) + ": malformed =yend trailer in article " + artname)
                        continue
                except Exception as e:
                    pass
                try:
                    bytes0.extend(inf)
                    pass
                except KeyboardInterrupt:
                    return
            if not headerok or not trailerok:  # or not partfound or partnr > 1:
                logger.warning(filename + ": wrong yenc structure detected")
                statusmsg = "yenc_structure_error"
                status = 0
            _, decodedcrc32, decoded = yenc.decode(bytes0)
            if not head_crc and not trail_crc:
                statusmsg = "no_pcrc32_error"
                logger.warning(filename + ": no pcrc32 detected")
                status = -1
            else:
                head_crc0 = None if not head_crc else head_crc.lower()
                trail_crc0 = None if not trail_crc else trail_crc.lower()
                crc32list = [head_crc0, trail_crc0]
                crc32 = decodedcrc32.lower()
                if crc32 not in crc32list:
                    # logger.warning(filename + ": CRC32 checksum error: " + crc32 + " / " + str(crc32list))
                    statusmsg = "crc32checksum_error: " + crc32 + " / " + str(crc32list)
                    status = -2
            bytesfinal.extend(decoded)
        if artsize0 != len(bytesfinal):
            statusmsg = "article file length wrong"
            status = -3
            logger.info("Wrong article length: should be " + str(artsize0) + ", actually was " + str(len(bytesfinal)))
        md5 = None
        full_filename = save_dir + filename
        try:
            if not os.path.isdir(save_dir):
                os.makedirs(save_dir)
            with open(full_filename, "wb") as f0:
                f0.write(bytesfinal)
                f0.flush()
                f0.close()
            logger.info(filename + " decoded and saved!")
            # calc hash for rars
            if filetype == "rar":
                md5 = 0  # calc_file_md5hash(save_dir + filename)
                if md5 == -1:
                    raise("Cannot calculate md5 hash")
                # logger.info(full_filename + " md5: " + str(md5))
        except Exception as e:
            statusmsg = "file_error"
            logger.error(str(e) + ": " + filename)
            status = -4
        logger.info(filename + " decoded with status " + str(status) + " / " + statusmsg)
        mp_result_queue0.put((filename, full_filename, filetype, status, statusmsg, md5))


def ParseNZB(nzbdir):
    cwd0 = os.getcwd()
    os.chdir(nzbdir)
    logger.info("Getting NZB files from " + nzbdir)
    for nzb in glob.glob("*.nzb"):
        pass
    try:
        tree = ET.parse(nzb)
        logger.info("Downloading NZB file: " + nzb)
    except Exception as e:
        logger.error(str(e) + ": please provide at least 1 NZB file!")
        return nzb, None
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
                bytescount = int(r0.attrib["bytes"])
                filelist.append((filename, int(nr0), bytescount))
            i -= 1
            if segfound:
                filelist.insert(0, (age, filetype, int(nr0)))
                filedic[hn] = filelist
    return nzb, filedic


# -------------------- Classes --------------------

# captures SIGINT / SIGTERM and closes down everything
class SigHandler():

    def __init__(self, servers, threads, mp_work_queue, mp_paroutqueue, logger):
        self.servers = servers
        self.logger = logger
        self.threads = threads
        self.mp_work_queue = mp_work_queue
        self.signal = False
        self.mp_paroutqueue = mp_paroutqueue

    def handler2(self, signal, frame):
        return

    def signalhandler(self, signal, frame):
        self.logger.warning("signalhandler: got SIGINT/SIGTERM!")
        self.signal = True
        self.logger.warning("signalhandler: sending stop to rarverifier")
        self.mp_paroutqueue.put(-1)
        self.logger.warning("signalhandler: stopping decoder processes")
        self.mp_work_queue.put(None)
        time.sleep(1)
        self.logger.warning("signalhandler: stopping download threads")
        for t, _ in self.threads:
            t.stop()
            t.join()
        self.logger.warning("signalhandler: closing all server connections")
        self.servers.close_all_connections()
        self.logger.warning("signalhandler: exiting")
        sys.exit()


# Does all the server stuff (open, close connctions) and contains all relevant
# data
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
                            logger.info("Opening connection # " + str(conn_nr) + "to server " + server_name)
                            if usessl:
                                nntpobj = nntplib.NNTP_SSL(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True, timeout=5)
                            else:
                                nntpobj = nntplib.NNTP(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True, timeout=5)
                            logger.info("Opened Connection #" + str(conn_nr) + " on server " + server_name0)
                            result = nntpobj
                            self.all_connections[idx] = (sn, cn, rt, nntpobj)
                            break
                        except Exception as e:
                            logger.error("Server " + server_name0 + " connect error: " + str(e))
                            self.all_connections[idx] = (sn, cn, rt, None)
                            break
                    else:
                        logger.error("Cannot get server config for server: " + server_name0)
                        self.all_connections[idx] = (sn, cn, rt, None)
                        break
        return result

    def close_all_connections(self):
        for (sn, cn, _, nobj) in self.all_connections:
            if nobj:
                try:
                    nobj.quit()
                    logger.warning("Closed connection #" + str(cn) + " on " + sn)
                except Exception as e:
                    logger.warning("Cannot quit server " + sn + ": " + str(e))

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


# This is the thread worker per connection to NNTP server
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
        self.name, self.conn_nr = self.connection
        self.idn = self.name + " #" + str(self.conn_nr)
        self.bytesdownloaded = 0
        self.last_timestamp = 0

    def stop(self):
        self.running = False

    # return status, info
    #        status = 1:  ok
    #                 0:  article not found
    #                -1:  retention not sufficient
    #                -2:  server connection error
    def download_article(self, article_name, article_age):
        sn, _ = self.connection
        bytesdownloaded = 0
        server_name, server_url, user, password, port, usessl, level, connections, retention = self.servers.get_single_server_config(sn)
        if retention < article_age * 0.95:
            logger.warning("Retention on " + server_name + " not sufficient for article " + article_name + ", return status = -1")
            return -1, None
        try:
            # resp_h, info_h = self.nntpobj.head(article_name)
            resp, info = self.nntpobj.body(article_name)
            if resp[:3] != "222":
                # if resp_h[:3] != "221" or resp[:3] != "222":
                logger.warning("Could not find " + article_name + "on " + self.idn + ", return status = 0")
                status = 0
                info0 = None
            else:
                status = 1
                info0 = [inf for inf in info.lines]
                bytesdownloaded = sum(len(i) for i in info0)
        except Exception as e:
            logger.error(str(e) + self.idn + " for article " + article_name + ", return status = -2")
            status = -2
            info0 = None
        return status, bytesdownloaded, info0

    def retry_connect(self):
        if self.nntpobj or not self.running:
            return
        idx = 0
        name, conn_nr = self.connection
        idn = name + " #" + str(conn_nr)
        logger.info("Server " + idn + " connecting ...")
        while idx < 5 and self.running:
            self.nntpobj = self.servers.open_connection(name, conn_nr)
            if self.nntpobj:
                logger.info("Server " + idn + " connected!")
                self.last_timestamp = time.time()
                return
            logger.warning("Could not connect to server " + idn + ", will retry in 5 sec.")
            time.sleep(5)
            idx += 1
        if not self.running:
            logger.warning("No connection retries anymore due to exiting")
        else:
            logger.error("Connect retries to " + idn + " failed!")

    def remove_from_remaining_servers(self, name, remaining_servers):
        next_servers = []
        for s in remaining_servers:
            addserver = s[:]
            try:
                addserver.remove(name)
            except Exception as e:
                pass
            if addserver:
                next_servers.append(addserver)
        return next_servers

    def run(self):
        logger.info(self.idn + " thread starting !")
        while True and self.running:
            self.retry_connect()
            if not self.nntpobj:
                time.sleep(5)
                continue
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
                logger.warning(self.idn + ": got poison pill!")
                break
            _, _, _, _, _, _, remaining_servers = test_article
            # no servers left
            # articlequeue = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
            if not remaining_servers:
                article = self.articlequeue.get()
                self.lock.release()
                self.resultqueue.put(article + (None,))
                continue
            if self.name not in remaining_servers[0]:
                self.lock.release()
                time.sleep(0.1)
                continue
            article = self.articlequeue.get()
            self.lock.release()
            filename, age, filetype, nr_articles, art_nr, art_name, remaining_servers1 = article
            # print("Downloading on server " + idn + ": + for article #" + str(art_nr), filename)
            status, bytesdownloaded, info = self.download_article(art_name, age)
            # if server connection error - disconnect
            if status == -2:
                # disconnect
                logger.warning("Stopping server " + self.idn)
                try:
                    self.nntpobj.quit()
                except Exception as e:
                    pass
                self.nntpobj = None
                # take next server
                next_servers = self.remove_from_remaining_servers(self.name, remaining_servers)
                next_servers.append([self.name])    # add current server to end of list
                logger.warning("Requeuing " + art_name + " on server " + self.idn)
                # requeue
                self.articlequeue.task_done()
                self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
                time.sleep(10)
                continue
            # if download successfull - put to resultqueue
            if status == 1:
                self.bytesdownloaded += bytesdownloaded
                # print("Download success on server " + idn + ": for article #" + str(art_nr), filename)
                self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, self.name, info))
                self.articlequeue.task_done()
            # if article could not be found on server / retention not good enough - requeue to other server
            if status in [0, -1]:
                next_servers = self.remove_from_remaining_servers(self.name, remaining_servers)
                self.articlequeue.task_done()
                if not next_servers:
                    logger.error("Download finally failed on server " + self.idn + ": for article #" + str(art_nr), next_servers)
                    self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, [], "failed"))
                else:
                    logger.warning("Download failed on server " + self.idn + ": for article #" + str(art_nr) + ", queueing: ", next_servers)
                    self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
        logger.info(self.idn + " exited!")


# Handles download of a NZB file
class Downloader():
    def __init__(self, servers, dirs, nzb, logger):
        self.nzb = nzb
        self.nzbdir = re.sub(r"[.]nzb$", "", self.nzb, flags=re.IGNORECASE) + "/"
        self.servers = servers
        self.lock = threading.Lock()
        self.level_servers = self.servers.level_servers
        self.all_connections = self.servers.all_connections
        self.articlequeue = queue.LifoQueue()
        self.resultqueue = queue.Queue()
        self.mp_work_queue = mp.Queue()
        self.mp_result_queue = mp.Queue()
        self.mp_parverify_outqueue = mp.Queue()
        self.mp_parverify_inqueue = mp.Queue()
        self.mp_unrarqueue = mp.Queue()
        self.threads = []
        self.dirs = dirs
        self.logger = logger
        # create dirs
        self.download_dir = dirs["incomplete"] + self.nzbdir + "_downloaded0/"
        self.verifiedrar_dir = dirs["incomplete"] + self.nzbdir + "_verifiedrars0/"
        self.unpack_dir = dirs["incomplete"] + self.nzbdir + "_unpack0/"
        self.main_dir = dirs["incomplete"] + self.nzbdir
        self.rename_dir = dirs["incomplete"] + self.nzbdir + "_renamed0/"
        try:
            if not os.path.isdir(self.unpack_dir):
                os.mkdir(self.unpack_dir)
            if not os.path.isdir(self.verifiedrar_dir):
                os.mkdir(self.verifiedrar_dir)
            if not os.path.isdir(self.main_dir):
                os.mkdir(self.main_dir)
            if not os.path.isdir(self.download_dir):
                os.mkdir(self.download_dir)
            if not os.path.isdir(self.rename_dir):
                os.mkdir(self.rename_dir)
        except Exception as e:
            logger.error(str(e) + " in creating dirs ...")

    def article_producer(self, articles, articlequeue):
        for article in articles:
            articlequeue.put(article)

    def make_allfilelist(self, filedic):
        allfilelist = []
        filetypecounter = {"rar": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "nfo": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "par2": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "par2vol": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "sfv": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "etc": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []}}
        idx = 0
        p2 = None
        savedirs = [self.download_dir, self.verifiedrar_dir, self.main_dir]
        
        for (filename, filelist) in filedic.items():
            sdir0 = ""
            file_already_exists = False
            for sdir0 in savedirs:
                for fname0 in glob.glob(sdir0 + "*"):
                    short_fn = fname0.split("/")[-1]
                    if short_fn == filename:
                        file_already_exists = True
                        break
                if file_already_exists:
                    break
            filename0 = sdir0 + filename
            age, filetype, nr_articles = filelist[0]
            filetype0 = filetype
            if re.search(r"[.]rar$", filename, flags=re.IGNORECASE):
                filetype0 = "rar"
                filetypecounter["rar"]["max"] += 1
                filetypecounter["rar"]["filelist"].append(filename)
            elif re.search(r"[.]nfo$", filename, flags=re.IGNORECASE):
                filetype0 = "nfo"
                filetypecounter["nfo"]["max"] += 1
                filetypecounter["nfo"]["filelist"].append(filename)
            elif re.search(r"[.]sfv$", filename, flags=re.IGNORECASE):
                filetype0 = "sfv"
                filetypecounter["sfv"]["max"] += 1
                filetypecounter["sfv"]["filelist"].append(filename)
            elif re.search(r"[.]par2$", filename, flags=re.IGNORECASE):
                if re.search(r"vol[0-9][0-9]*[+]", filename, flags=re.IGNORECASE):
                    filetype0 = "par2vol"
                    filetypecounter["par2vol"]["max"] += 1
                    filetypecounter["par2vol"]["filelist"].append(filename)
                else:
                    filetype0 = "par2"
                    filetypecounter["par2"]["max"] += 1
                    filetypecounter["par2"]["filelist"].append(filename)
                    if file_already_exists:
                        p2 = par2lib.Par2File(filename0)
            else:
                filetype0 = "etc"
                filetypecounter["etc"]["max"] += 1
                filetypecounter["etc"]["filelist"].append(filename)
            if file_already_exists:
                filetypecounter[filetype0]["counter"] += 1
                md5 = calc_file_md5hash(filename0)
                filetypecounter[filetype0]["loadedfiles"].append((filename, filename0, md5))
                continue
            allfilelist.append([(filename, age, filetype0, nr_articles)])
            for i, f in enumerate(filelist):
                if i > 0:
                    fn, nr, bytescount = f
                    allok = True
                    # check for duplicate art.
                    if len(allfilelist[idx]) > 2:
                        for i1, art in enumerate(allfilelist[idx]):
                            if i1 > 1:
                                nr1, fn1, _ = art
                                if nr1 == nr:
                                    allok = False
                                    break
                    if allok:
                        allfilelist[idx].append((nr, fn, bytescount))
            idx += 1
        return allfilelist, filetypecounter, p2

    def getbytescount(self, filelist):
        # generate all articles and files
        bytescount0 = 0
        for file_articles in filelist:
            # iterate over all articles in file
            for i, art0 in enumerate(file_articles):
                if i == 0:
                    continue
                _, _, art_bytescount = art0
                bytescount0 += art_bytescount
        bytescount0 = bytescount0 / (1024 * 1024 * 1024)
        return bytescount0

    def inject_articles(self, ftypes, filelist, files0, infolist0, bytescount0_0):
        # generate all articles and files
        files = files0
        infolist = infolist0
        bytescount0 = bytescount0_0
        for j, file_articles in enumerate(reversed(filelist)):
            # iterate over all articles in file
            filename, age, filetype, nr_articles = file_articles[0]
            # check if file already exists in "incomplete"
            if filetype in ftypes:
                level_servers = self.get_level_servers(age)
                files[filename] = (nr_articles, age, filetype, False, True)
                infolist[filename] = [None] * nr_articles
                for i, art0 in enumerate(file_articles):
                    if i == 0:
                        continue
                    art_nr, art_name, art_bytescount = art0
                    bytescount0 += art_bytescount
                    q = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
                    self.articlequeue.put(q)
        bytescount0 = bytescount0 / (1024 * 1024 * 1024)
        return files, infolist, bytescount0

    def process_resultqueue(self, avgmiblist00, infolist00, files00):
        # read resultqueue + distribute to files
        newresult = False
        avgmiblist = avgmiblist00
        infolist = infolist00
        files = files00
        while True:
            try:
                resultarticle = self.resultqueue.get_nowait()
                self.resultqueue.task_done()
                filename, age, filetype, nr_articles, art_nr, art_name, download_server, inf0 = resultarticle
                bytesdownloaded = sum(len(i) for i in inf0)
                avgmiblist.append((time.time(), bytesdownloaded, download_server))
                try:
                    infolist[filename][art_nr - 1] = inf0
                    newresult = True
                except TypeError:
                    continue
                # check if file is completed and put to mp_queue/decode in case
                (f_nr_articles, f_age, f_filetype, f_done, f_failed) = files[filename]
                if not f_done and len([inf for inf in infolist[filename] if inf]) == f_nr_articles:        # check for failed!! todo!!
                    failed0 = False
                    if "failed" in infolist[filename]:
                        logger.error(filename + "failed!!")
                        failed0 = True
                    inflist0 = infolist[filename][:]
                    self.mp_work_queue.put((inflist0, self.download_dir, filename, filetype))
                    files[filename] = (f_nr_articles, f_age, f_filetype, True, failed0)
                    infolist[filename] = None
                    logger.info("All articles for " + filename + " downloaded, calling mp.decode ...")
            except KeyError:
                pass
            except queue.Empty:
                break
        return newresult, avgmiblist, infolist, files

    def display_console_connection_data(self, bytescount00, availmem00, avgmiblist00, filetypecounter00):
        avgmiblist = avgmiblist00
        max_mem_needed = 0
        bytescount0 = bytescount00
        bytescount0 += 0.00001
        availmem0 = availmem00
        # get Mib downloaded
        if len(avgmiblist) > 50:
            del avgmiblist[0]
        if len(avgmiblist) > 10:
            avgmib_dic = {}
            for (server_name, _, _, _, _, _, _, _, _) in self.servers.server_config:
                bytescountlist = [bytescount for (_, bytescount, download_server0) in avgmiblist if server_name == download_server0]
                if len(bytescountlist) > 2:
                    avgmib_db = sum(bytescountlist)
                    avgmib_mint = min([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    avgmib_maxt = max([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    # print(avgmib_maxt, avgmib_mint)
                    avgmib_dic[server_name] = (avgmib_db / (avgmib_maxt - avgmib_mint)) / (1024 * 1024) * 8
                else:
                    avgmib_dic[server_name] = 0
            for server_name, avgmib in avgmib_dic.items():
                mem_needed = ((psutil.virtual_memory()[0] - psutil.virtual_memory()[1]) - availmem0) / (1024 * 1024 * 1024)
                if mem_needed > max_mem_needed:
                    max_mem_needed = mem_needed
        # set in all threads servers alive timestamps
        # check if server is longer off > 120 sec, if yes, kill thread & stop server
        try:
            print("MBit/sec.: " + str([sn + ": " + str(int(av)) + "  " for sn, av in avgmib_dic.items()]) + " max. mem_needed: "
                  + "{0:.3f}".format(max_mem_needed) + " GB                 ")
        except UnboundLocalError:
            print("MBit/sec.: --- max. mem_needed: " + str(max_mem_needed) + " GB                ")
        gbdown0 = 0
        mbitsec0 = 0
        for k, (t, last_timestamp) in enumerate(self.threads):
            gbdown = t.bytesdownloaded / (1024 * 1024 * 1024)
            gbdown0 += gbdown
            gbdown_str = "{0:.3f}".format(gbdown)
            mbitsec = (t.bytesdownloaded / (time.time() - t.last_timestamp)) / (1024 * 1024) * 8
            mbitsec0 += mbitsec
            mbitsec_str = "{0:.1f}".format(mbitsec)
            print(t.idn + ": Total - " + gbdown_str + " GB" + " | MBit/sec. - " + mbitsec_str + "                        ")
        print("-" * 60)
        gbdown0_str = "{0:.3f}".format(gbdown0)
        print("Total GB: " + gbdown0_str + " = " + "{0:.1f}".format((gbdown0 / bytescount0) * 100) + "% of total "
              + "{0:.2f}".format(bytescount0) + "GB | MBit/sec. - " + "{0:.1f}".format(mbitsec0) + "             ")
        for key, item in filetypecounter00.items():
            print(key + ": " + str(item["counter"]) + "/" + str(item["max"]) + ", ", end="")
        print()
        for _ in range(len(self.threads) + 4):
            sys.stdout.write("\033[F")

    # main download routine
    def download_and_process(self, filedic):
        # directories
        #    incomplete/_downloaded0:   all files go straight to here after download
        #    incomplete/_verifiedrar0:  downloaded & verfied rars, ready to unrar
        #    incomplete/_unpack0:       unpacked verified rars
        #    incomplete:                final directory before moving to complete

        availmem0 = psutil.virtual_memory()[0] - psutil.virtual_memory()[1]
        allfileslist, filetypecounter, p2 = self.make_allfilelist(filedic)
        logger.info("Downloading articles for: " + self.nzb)

        # overall GB
        bytescount0 = self.getbytescount(allfileslist)

        # start decoder thread
        mpp = mp.Process(target=decode_articles, args=(self.mp_work_queue, self.mp_result_queue, self.logger, ))
        mpp.start()

        # start par2verify thread
        mpp_par = mp.Process(target=par2lib.par_verifier, args=(self.mp_parverify_outqueue, self.mp_parverify_inqueue,
                             self.download_dir, self.verifiedrar_dir, self.main_dir, self.logger, filetypecounter, p2, ))
        mpp_par.start()

        # start partial_unrar thread
        mpp_unrar = mp.Process(target=par2lib.partial_unrar, args=(self.mp_unrarqueue, self.verifiedrar_dir, self.unpack_dir,
                                                                   self.logger, ))
        mpp_unrar.start()

        # register sigint/sigterm handlers
        self.sighandler = SigHandler(self.servers, self.threads, self.mp_work_queue, self.mp_parverify_outqueue, self.logger)
        signal.signal(signal.SIGINT, self.sighandler.signalhandler)
        signal.signal(signal.SIGTERM, self.sighandler.signalhandler)

        avgmiblist = []
        status = 0        # 0 = running, 1 = exited successfull, -1 = exited cannot unrar
        inject_set0 = ["par2", "rar", "sfv", "nfo", "etc"]
        files = {}
        infolist = {}
        files, infolist, bytescount0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
        loadpar2vols = False
        p2 = None

        # if files in queue, start connection worker threads
        if allfileslist:
            for sn, scon, _, _ in self.all_connections:
                t = ConnectionWorker(self.lock, (sn, scon), self.articlequeue, self.resultqueue, self.servers)
                self.threads.append((t, time.time()))
                t.start()

        # main download & processing loop
        while True and not self.sighandler.signal:

            # get mp_result_queue
            while True:
                try:
                    filename, full_filename, filetype, status, statusmsg, md5 = self.mp_result_queue.get_nowait()
                    filetypecounter[filetype]["counter"] += 1
                    filetypecounter[filetype]["loadedfiles"].append((filename, full_filename, md5))
                    if (filetype == "par2" or filetype == "par2vol") and not p2:
                        p2 = par2lib.Par2File(full_filename)
                        logger.info("Sending " + filename + "-p2 object to parverify_queue")
                        self.mp_parverify_outqueue.put(p2)
                except queue.Empty:
                    break

            # get mp_parverify_inqueue
            if not loadpar2vols:
                while True:
                    try:
                        loadpar2vols, parverify_status = self.mp_parverify_inqueue.get_nowait()
                    except queue.Empty:
                        break
                if loadpar2vols:
                    logger.info("Queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                    bytescount0 += bytescount00
            # if all downloaded postprocess
            dobreak = True
            for filetype, item in filetypecounter.items():
                if filetype == "par2vol" and not loadpar2vols:
                    continue
                if filetypecounter[filetype]["counter"] < filetypecounter[filetype]["max"]:
                    dobreak = False
                    break

            # read resultqueue + decode via mp
            newresult, avgmiblist, infolist, files = self.process_resultqueue(avgmiblist, infolist, files)

            # disply connection speeds in console
            self.display_console_connection_data(bytescount0, availmem0, avgmiblist, filetypecounter)

            if dobreak:
                break

            # update threads timestamps (if alive)
            # if thread is dead longer than 120 sec - kill it
            for k, (t, last_timestamp) in enumerate(self.threads):
                if t.isAlive() and t.nntpobj:
                        last_timestamp = time.time()
                        self.threads[k] = (t, last_timestamp)
                if t.isAlive() and time.time() - last_timestamp > 120:
                    t.stop()
                    t.join()
                    try:
                        t.nntopj.quit()
                        t.nntpobj = None
                    except Exception as e:
                        logger.error(str(e))

            # if all servers down (= no WAN)
            if len([t for t, _ in self.threads if t.isAlive()]) == 0:
                status = -1
                break
            time.sleep(0.1)

        # get final par2 repair status
        parverify_status = 0
        logger.info("Waiting for receiving ok from par_verifier")
        while True:
            try:
                endcode, parverify_status = self.mp_parverify_inqueue.get_nowait()
                if endcode == -9999:
                    break
            except queue.Empty:
                pass
        if parverify_status == 1:
            logger.info("All rar files ok / repaired!")
        elif parverify_status == -1:
            logger.error("Some rar files were corrupt/could not be repaired!")
        else:
            logger.error("Got no reasonable final state from par_verifier ...")

        # get final unrar status
        unrar_status = 0
        logger.info("Waiting for receiving ok from unrar")
        while True:
            try:
                unrar_status, statmsg = self.mp_unrarqueue.get_nowait()
                break
            except queue.Empty:
                pass
        if unrar_status == 0:
            logger.info("Unrar - " + statmsg)
        else:
            logger.error("Unrar - " + statmsg)

        # todo:
        #   clean directories and move to final directory

        # clean up
        logger.info("cleaning up ...")
        self.mp_work_queue.put(None)
        self.resultqueue.join()
        self.articlequeue.join()
        for t, _ in self.threads:
            t.stop()
            t.join()
        self.servers.close_all_connections()
        return status

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


# -------------------- main --------------------

if __name__ == '__main__':

    print("Welcome to ginzibix 0.1-alpha, binary usenet downloader")
    print("-" * 60)

    cfg = configparser.ConfigParser()
    cfg.read(dirs["config"] + "/ginzibix.config")

    # get servers
    servers = Servers(cfg)
    if not servers:
        logger.error("At least one server has to be provided, exiting!")
        sys.exit()

    nzb, filedic = ParseNZB(dirs["nzb"])
    if filedic:
        t0 = time.time()
        dl = Downloader(servers, dirs, nzb, logger)
        status = dl.download_and_process(filedic)
        print("\n" + nzb + " downloaded in " + str(int(time.time() - t0)) + " sec. with status " + str(status))

    logger.warning("### EXITED GINZIBIX ###")
