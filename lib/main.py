#!/home/stephan/.virtualenvs/nntp/bin/python

import time
import sys
import os
import queue
import signal
import multiprocessing as mp
import logging
import psutil
import re
import threading
from threading import Thread
import shutil
import glob
import zmq
from .renamer import renamer
from .par_verifier import par_verifier
from .par2lib import Par2File
from .partial_unrar import partial_unrar
from .nzb_parser import ParseNZB
from .article_decoder import decode_articles
from .passworded_rars import is_rar_password_protected, get_password
from .connections import ConnectionWorker
from .server import Servers
import dill
from statistics import mean
from .aux import PWDBSender
import inspect

lpref = __name__.split("lib.")[-1] + " - "


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


_ftypes = ["etc", "rar", "sfv", "par2", "par2vol"]


def remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger):
    nzbdirname = re.sub(r"[.]nzb$", "", deleted_nzb_name0, flags=re.IGNORECASE) + "/"
    # delete nzb from .ginzibix/nzb0
    try:
        os.remove(dirs["nzb"] + deleted_nzb_name0)
        logger.debug(whoami() + ": deleted NZB " + deleted_nzb_name0 + " from NZB dir")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # remove from db
    pwdb.exc("db_nzb_delete", [deleted_nzb_name0], {})
    # pwdb.db_nzb_delete(deleted_nzb_name0)
    # remove incomplete/$nzb_name
    try:
        shutil.rmtree(dirs["incomplete"] + nzbdirname)
        logger.debug(whoami() + ": deleted incomplete dir for " + deleted_nzb_name0)
    except Exception as e:
        logger.debug(whoami() + str(e))


class SigHandler_Main:

    def __init__(self, mpp, ct, mp_work_queue, resultqueue, articlequeue, pwdb, logger):
        self.logger = logger
        self.ct = ct
        self.mpp = mpp
        self.mp_work_queue = mp_work_queue
        self.resultqueue = resultqueue
        self.articlequeue = articlequeue
        self.main_dir = None
        self.nzbname = None
        self.pwdb = pwdb

    def shutdown(self):
        self.logger.info(whoami() + "starting shutdown sequence ...")
        f = open('/dev/null', 'w')
        sys.stdout = f
        # just log mpp pids
        for key, item in self.mpp.items():
            if item:
                item_pid = str(item.pid)
            else:
                item_pid = "-"
            self.logger.debug(whoami() + "MPP " + key + ", pid = " + item_pid)
        # 1. clear articlequeue
        self.logger.debug(whoami() + "clearing articlequeue")
        while True:
            try:
                self.articlequeue.get_nowait()
                self.articlequeue.task_done()
            except (queue.Empty, EOFError):
                break
        self.articlequeue.join()
        # 2. wait for all downloads to be finished
        self.logger.debug(whoami() + "waiting for all remaining articles to be downloaded")
        dl_not_done_yet = True
        while dl_not_done_yet:
            dl_not_done_yet = False
            for t, _ in self.ct.threads:
                if not t.is_download_done():
                    dl_not_done_yet = True
                    break
            if dl_not_done_yet:
                time.sleep(0.2)
        # 3. stop decoder
        mpid = None
        try:
            if self.mpp["decoder"]:
                mpid = self.mpp["decoder"].pid
            if mpid:
                self.logger.debug(whoami() + "terminating decoder")
                try:
                    os.kill(self.mpp["decoder"].pid, signal.SIGKILL)
                    self.mpp["decoder"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 4. clear mp_work_queue
        self.logger.debug(whoami() + "clearing mp_work_queue")
        while True:
            try:
                self.mp_work_queue.get_nowait()
            except (queue.Empty, EOFError):
                break
        # 5. write resultqueue to file
        if self.main_dir and self.nzbname:
            self.logger.debug(whoami() + "writing resultqueue to .gzbx file")
            time.sleep(0.5)
            bytes_in_resultqueue = write_resultqueue_to_file(self.resultqueue, self.main_dir, self.logger)
            # self.pwdb.db_nzb_set_bytes_in_resultqueue(self.nzbname, bytes_in_resultqueue)
            self.pwdb.exc("db_nzb_set_bytes_in_resultqueue", [self.nzbname, bytes_in_resultqueue], {})
        # 6. stop unrarer
        try:
            mpid = None
            if self.mpp["unrarer"]:
                mpid = self.mpp["unrarer"].pid
            if mpid:
                # if self.mpp["unrarer"].pid:
                self.logger.debug(whoami() + "terminating unrarer")
                try:
                    os.kill(self.mpp["unrarer"].pid, signal.SIGKILL)
                    self.mpp["unrarer"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 7. stop rar_verifier
        try:
            mpid = None
            if self.mpp["verifier"]:
                mpid = self.mpp["verifier"].pid
            if mpid:
                self.logger.debug(whoami() + "terminating rar_verifier")
                try:
                    os.kill(self.mpp["verifier"].pid, signal.SIGKILL)
                    self.mpp["verifier"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 8. stop mpp_renamer
        try:
            mpid = None
            if self.mpp["renamer"]:
                mpid = self.mpp["renamer"].pid
            if mpid:
                self.logger.debug(whoami() + "terminating renamer")
                try:
                    os.kill(self.mpp["renamer"].pid, signal.SIGKILL)
                    self.mpp["renamer"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 9. stop nzbparser
        try:
            mpid = None
            if self.mpp["nzbparser"]:
                mpid = self.mpp["nzbparser"].pid
            if mpid:
                self.logger.debug(whoami() + "terminating nzb_parser")
                try:
                    os.kill(self.mpp["nzbparser"].pid, signal.SIGKILL)
                    self.mpp["nzbparser"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 10. threads + servers
        if self.ct.threads:
            self.logger.debug(whoami() + "stopping download threads")
            for t, _ in self.ct.threads:
                t.stop()
                t.join()
        try:
            if self.ct.servers:
                self.logger.debug(whoami() + "closing all server connections")
                self.servers.close_all_connections()
        except Exception:
            pass
        self.logger.info(whoami() + "shutdown sequence finished, exiting!")
        # !!!
        # !!! THIS IS IMPORTANT
        # !!! goodbye to gpeewee must be the last command from main
        # !!!
        self.pwdb.exc("set_exit_goodbye_from_main", [], {})
        sys.exit()

    def sighandler(self, a, b):
        self.shutdown()


class GUI_Connector(Thread):
    def __init__(self, lock, dirs, logger, cfg):
        Thread.__init__(self)
        self.daemon = True
        self.dirs = dirs
        self.pwdb = PWDBSender(cfg)
        self.cfg = cfg
        try:
            self.port = self.cfg["OPTIONS"]["PORT"]
            assert(int(self.port) > 1024 and int(self.port) <= 65535)
        except Exception as e:
            self.logger.debug(whoami() + str(e) + ", setting port to default 36603")
            self.port = "36603"
        self.data = None
        self.nzbname = None
        self.pwdb_msg = (None, None)
        self.logger = logger
        self.lock = lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + self.port)
        self.threads = []
        self.server_config = None
        self.dl_running = True
        self.status = "idle"
        self.first_has_changed = False
        self.deleted_nzb_name = None
        self.old_t = 0
        self.oldbytes0 = 0
        self.send_data = True
        self.sorted_nzbs = None
        self.dlconfig = None
        self.netstatlist = []
        self.last_update_for_gui = 0

    def set_health(self, article_health, connection_health):
        with self.lock:
            self.article_health = article_health
            self.connection_health = connection_health

    def set_data(self, data, threads, server_config, status, dlconfig):
        if data:
            with self.lock:
                bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
                self.data = data
                self.nzbname = nzbname
                # self.pwdb_msg = self.pwdb.db_msg_get(nzbname)
                self.pwdb_msg = self.pwdb.exc("db_msg_get", [nzbname], {})
                self.server_config = server_config
                self.status = status
                self.dlconfig = dlconfig
                self.threads = []
                for k, (t, last_timestamp) in enumerate(threads):
                    append_tuple = (t.bytesdownloaded, t.last_timestamp, t.idn, t.bandwidth_bytes)
                    self.threads.append(append_tuple)
                self.send_data = True
        else:
            with self.lock:
                self.send_data = False

    def get_netstat(self):
        pid = os.getpid()
        with open("/proc/" + str(pid) + "/net/netstat", "r") as f:
            bytes0 = None
            for line in f:
                if line.startswith("IpExt"):
                    line0 = line.split(" ")
                    try:
                        bytes0 = int(line0[7])
                        break
                    except Exception:
                        pass
        if bytes0:
            dt = time.time() - self.old_t
            if dt == 0:
                dt = 0.001
            self.old_t = time.time()
            mbitcurr = ((bytes0 - self.oldbytes0) / dt) / (1024 * 1024) * 8
            self.oldbytes0 = bytes0
            self.netstatlist.append(mbitcurr)
            if len(self.netstatlist) > 4:
                del self.netstatlist[0]
            return mean(self.netstatlist)
        else:
            return 0

    def get_data(self):
        ret0 = (None, None, None, None, None, None, None, None, None, None, None, None)
        # lastt = self.pwdb.get_last_update_for_gui()
        lastt = self.pwdb.exc("get_last_update_for_gui", [], {})
        if lastt > self.last_update_for_gui:
            self.send_data = True
            # full_data_for_gui = self.pwdb.get_all_data_for_gui()
            full_data_for_gui = self.pwdb.exc("get_all_data_for_gui", [], {})
            self.last_update_for_gui = lastt
        else:
            full_data_for_gui = None
        if self.send_data:
            with self.lock:
                try:
                    ret0 = (self.data, self.pwdb_msg, self.server_config, self.threads, self.dl_running, self.status,
                            self.get_netstat(), self.sorted_nzbs, self.article_health, self.connection_health, self.dlconfig, full_data_for_gui)
                except Exception as e:
                    self.logger.error("GUI_Connector: " + str(e))
        return ret0

    def has_first_nzb_changed(self):
        res = self.first_has_changed
        self.first_has_changed = False
        return res

    def has_nzb_been_deleted(self, delete=False):
        res = self.deleted_nzb_name
        if delete:
            self.deleted_nzb_name = None
        return res

    def run(self):
        while True:
            try:
                msg, datarec = self.socket.recv_pyobj()
            except Exception as e:
                self.logger.error(whoami() + str(e))
                try:
                    self.socket.send_pyobj(("NOOK", None))
                except Exception as e:
                    self.logger.error(whoami() + str(e))
            if msg == "PWDB":
                try:
                    self.socket.send_pyobj(("OK", None))
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                with self.lock:
                    self.sorted_nzbs = datarec
            elif msg == "REQ":
                getdata = self.get_data()
                gd1, _, _, _, _, _, _, sortednzbs, _, _, _, _ = getdata
                if gd1:
                    sendtuple = ("DL_DATA", getdata)
                else:
                    sendtuple = ("NOOK", getdata)
                try:
                    self.socket.send_pyobj(sendtuple)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
            elif msg == "SET_PAUSE":     # pause downloads
                try:
                    self.socket.send_pyobj(("SET_PAUSE_OK", None))
                    with self.lock:
                        self.dl_running = False
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            elif msg == "SET_RESUME":    # resume downloads
                try:
                    self.socket.send_pyobj(("SET_RESUME_OK", None))
                    self.dl_running = True
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            elif msg == "SET_DELETE":
                try:
                    self.socket.send_pyobj(("SET_DELETE_OK", None))
                    # first_has_changed0, deleted_nzb_name0 = self.pwdb.set_nzbs_prios(datarec, delete=True)
                    first_has_changed0, deleted_nzb_name0 = self.pwdb.exc("set_nzbs_prios", [datarec], {"delete": True})
                    if deleted_nzb_name0 and not first_has_changed0:
                        remove_nzb_files_and_db(deleted_nzb_name0, self.dirs, self.pwdb, self.logger)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                if first_has_changed0:
                    self.first_has_changed = first_has_changed0
                    self.deleted_nzb_name = deleted_nzb_name0
                continue
            elif msg == "SET_NZB_ORDER":
                try:
                    self.socket.send_pyobj(("SET_NZBORDER_OK", None))
                    self.first_has_changed, _ = self.pwdb.exc("set_nzbs_prios", [datarec], {"delete": False})
                    # self.first_has_changed, _ = self.pwdb.set_nzbs_prios(datarec, delete=False)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            else:
                try:
                    self.socket.send_pyobj(("NOOK", None))
                except Exception as e:
                    self.logger.debug(whoami() + str(e) + ", received msg: " + str(msg))
                continue


# Handles download of a NZB file
class Downloader():
    def __init__(self, cfg, dirs, ct, mp_work_queue, sighandler, mpp, guiconnector, logger):
        self.cfg = cfg
        self.pwdb = PWDBSender(cfg)
        self.articlequeue = ct.articlequeue
        self.resultqueue = ct.resultqueue
        self.mp_work_queue = mp_work_queue
        self.mp_result_queue = mp.Queue()
        self.mp_parverify_outqueue = mp.Queue()
        self.mp_parverify_inqueue = mp.Queue()
        self.mp_unrarqueue = mp.Queue()
        self.mp_nzbparser_outqueue = mp.Queue()
        self.mp_nzbparser_inqueue = mp.Queue()
        self.ct = ct
        self.dirs = dirs
        self.logger = logger
        self.sighandler = sighandler
        self.mpp = mpp
        self.guiconnector = guiconnector
        self.resqlist = []
        self.article_health = 1
        self.connection_health = 1
        self.contains_par_files = False
        self.read_cfg()
        if self.pw_file:
            try:
                self.logger.debug(whoami() + "as a first test, open password file")
                with open(self.pw_file, "r") as f0:
                    f0.readlines()
                self.logger.info(whoami() + "password file is available")
            except Exception as e:
                self.logger.warning(whoami() + str(e) + ": cannot open pw file, setting to None")
                self.pw_file = None

    def read_cfg(self):
        # pw_file
        try:
            self.pw_file = self.dirs["main"] + self.cfg["OPTIONS"]["PW_FILE"]
            self.logger.debug(whoami() + "password file is: " + self.pw_file)
        except Exception as e:
            self.logger.debug(whoami() + str(e) + ": no pw file provided!")
            self.pw_file = None
        # critical connection health
        try:
            self.crit_conn_health = float(self.cfg["OPTIONS"]["crit_conn_health"])
            assert(self.crit_conn_health > 0 and self.crit_conn_health <= 1)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            self.crit_conn_health = 0.70
        # critical health with par files avail.
        try:
            self.crit_art_health_w_par = float(self.cfg["OPTIONS"]["crit_art_health_w_par"])
            assert(self.crit_art_health_w_par > 0 and self.crit_art_health_w_par <= 1)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            self.crit_art_health_w_par = 0.98
        # critical health without par files avail.
        try:
            self.crit_art_health_wo_par = float(self.cfg["OPTIONS"]["crit_art_health_wo_par"])
            assert(self.crit_art_health_wo_par > 0 and self.crit_art_health_wo_par <= 1)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            self.crit_art_health_wo_par = 0.999

    def serverconfig(self):
        if self.contains_par_files:
            return (self.crit_art_health_w_par, self.crit_conn_health)
        else:
            return (self.crit_art_health_wo_par, self.crit_conn_health)

    def make_dirs(self, nzb):
        self.nzb = nzb
        self.nzbdir = re.sub(r"[.]nzb$", "", self.nzb, flags=re.IGNORECASE) + "/"
        self.download_dir = self.dirs["incomplete"] + self.nzbdir + "_downloaded0/"
        self.verifiedrar_dir = self.dirs["incomplete"] + self.nzbdir + "_verifiedrars0/"
        self.unpack_dir = self.dirs["incomplete"] + self.nzbdir + "_unpack0/"
        self.main_dir = self.dirs["incomplete"] + self.nzbdir
        self.rename_dir = self.dirs["incomplete"] + self.nzbdir + "_renamed0/"
        try:
            if not os.path.isdir(self.dirs["incomplete"]):
                os.mkdir(self.dirs["incomplete"])
            if not os.path.isdir(self.main_dir):
                os.mkdir(self.main_dir)
            if not os.path.isdir(self.unpack_dir):
                os.mkdir(self.unpack_dir)
            if not os.path.isdir(self.verifiedrar_dir):
                os.mkdir(self.verifiedrar_dir)
            if not os.path.isdir(self.download_dir):
                os.mkdir(self.download_dir)
            if not os.path.isdir(self.rename_dir):
                os.mkdir(self.rename_dir)
        except Exception as e:
            self.logger.error(whoami() + str(e) + " in creating dirs!")
            return -1
        time.sleep(1)
        return 1

    def make_complete_dir(self):
        self.complete_dir = self.dirs["complete"] + self.nzbdir
        try:
            if not os.path.isdir(self.dirs["complete"]):
                os.mkdir(self.dirs["complete"])
        except Exception as e:
            self.logger.error(str(e) + " in creating complete ...")
            return False
        if os.path.isdir(self.complete_dir):
            try:
                shutil.rmtree(self.complete_dir)
            except Exception as e:
                self.logger.error(str(e) + " in deleting complete_dir ...")
                return False
        try:
            if not os.path.isdir(self.complete_dir):
                os.mkdir(self.complete_dir)
            time.sleep(1)
            return True
        except Exception as e:
            self.logger.error(str(e) + " in creating dirs ...")
            return False

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
        article_count = 0
        for f in ftypes:
            for j, file_articles in enumerate(reversed(filelist)):
                # iterate over all articles in file
                filename, age, filetype, nr_articles = file_articles[0]
                if filetype == f:
                    level_servers = self.get_level_servers(age)
                    files[filename] = (nr_articles, age, filetype, False, True)
                    infolist[filename] = [None] * nr_articles
                    # self.pwdb.db_file_update_status(filename, 1)   # status do downloading
                    self.pwdb.exc("db_file_update_status", [filename, 1], {})   # status do downloading
                    bytescount_file = 0
                    for i, art0 in enumerate(file_articles):
                        if i == 0:
                            continue
                        art_nr, art_name, art_bytescount = art0
                        art_found = False
                        if self.resqlist:
                            for fn_r, age_r, ft_r, nr_art_r, art_nr_r, art_name_r, download_server_r, inf0_r, _ in self.resqlist:
                                if art_name == art_name_r:
                                    art_found = True
                                    break
                        if art_found:
                            # put to resultqueue:
                            self.resultqueue.put((fn_r, age_r, ft_r, nr_art_r, art_nr_r, art_name_r, download_server_r, inf0_r, False))
                        else:
                            bytescount0 += art_bytescount
                            bytescount_file += art_bytescount
                            q = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
                            self.articlequeue.put(q)
                        article_count += 1
                    # self.logger.debug("------ " + filename + " " + str(filetype) + " " + str(bytescount_file / (1024 * 1024 * 1024)))
        bytescount0 = bytescount0 / (1024 * 1024 * 1024)
        return files, infolist, bytescount0, article_count

    def all_queues_are_empty(self):
        articlequeue_empty = self.articlequeue.empty()
        resultqueue_empty = self.resultqueue.empty()
        mpworkqueue_empty = self.mp_work_queue.empty()
        return (articlequeue_empty and resultqueue_empty and mpworkqueue_empty)

    def process_resultqueue(self, avgmiblist00, infolist00, files00):
        # read resultqueue + distribute to files
        empty_yenc_article = [b"=ybegin line=128 size=14 name=ginzi.txt",
                              b'\x9E\x92\x93\x9D\x4A\x93\x9D\x4A\x8F\x97\x9A\x9E\xA3\x34\x0D\x0A',
                              b"=yend size=14 crc32=8111111c"]
        newresult = False
        avgmiblist = avgmiblist00
        infolist = infolist00
        files = files00
        failed = 0
        while True:
            try:
                resultarticle = self.resultqueue.get_nowait()
                self.resultqueue.task_done()
                filename, age, filetype, nr_articles, art_nr, art_name, download_server, inf0, add_bytes = resultarticle
                if inf0 == "failed":
                    failed += 1
                    inf0 = empty_yenc_article
                    self.logger.error(filename + "/" + art_name + ": failed!!")
                bytesdownloaded = 0
                if add_bytes:
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
                    if b"name=ginzi.txt" in infolist[filename][0]:
                        failed0 = True
                        self.logger.error(filename + ": failed!!")
                    inflist0 = infolist[filename][:]
                    self.mp_work_queue.put((inflist0, self.download_dir, filename, filetype))
                    files[filename] = (f_nr_articles, f_age, f_filetype, True, failed0)
                    infolist[filename] = None
                    self.logger.debug(whoami() + "All articles for " + filename + " downloaded, calling mp.decode ...")
            except KeyError:
                pass
            except (queue.Empty, EOFError):
                break
        if len(avgmiblist) > 50:
            avgmiblist = avgmiblist[:50]
        return newresult, avgmiblist, infolist, files, failed

    def connection_thread_health(self):
        nothreads = 0
        nodownthreads = 0
        for t, _ in self.ct.threads:
            nothreads += 1
            if t.connectionstate == -1:
                nodownthreads += 1
        # self.logger.debug(">>>>" + str(nothreads) + " / " + str(nodownthreads))
        return 1 - nodownthreads / (nothreads + 0.00001)

    def restart_all_threads(self):
        self.logger.debug(whoami() + "connection-restart: shutting down")
        if self.ct.threads:
            for t, _ in self.ct.threads:
                t.stop()
                t.join()
        try:
            if self.ct.servers:
                self.servers.close_all_connections()
        except Exception:
            pass
        del self.ct.servers
        del self.ct.threads
        time.sleep(3)
        self.ct.servers = None
        self.ct.threads = []
        self.logger.debug(whoami() + "connection-restart: restarting")
        try:
            self.ct.init_servers()
            self.ct.start_threads()
            self.sighandler.servers = self.ct.servers
            self.logger.debug(whoami() + "connection-restart: servers restarted")
        except Exception as e:
            self.logger.warning(whoami() + "cannot restart servers + threads")
            # !!! todo: return to main loop here with status = failed

    # postprocessor
    def postprocess_nzb(self, nzbname, downloaddata):
        self.guiconnector.set_data(downloaddata, self.ct.threads, self.ct.servers.server_config, "postprocessing", self.serverconfig())
        bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health, overall_size, already_downloaded_size = downloaddata
        # self.pwdb.db_msg_insert(nzbname, "starting postprocess", "info")
        self.pwdb.exc("db_msg_insert", [nzbname, "starting postprocess", "info"], {})
        while True:
            try:
                self.articlequeue.get_nowait()
                self.articlequeue.task_done()
            except (queue.Empty, EOFError):
                break
        self.articlequeue.join()
        while True:
            try:
                self.resultqueue.get_nowait()
                self.resultqueue.task_done()
            except (queue.Empty, EOFError):
                break
        self.resultqueue.join()
        # join renamer
        if self.mpp["renamer"]:
            try:
                # to do: loop over downloaded and wait until empty
                self.logger.debug(whoami() + "Waiting for renamer.py clearing download dir")
                while True:
                    for _, _, fs in os.walk(self.download_dir):
                        if not fs:
                            break
                    else:
                        time.sleep(1)
                        continue
                    break
                self.logger.debug(whoami() + "Download dir empty!")
                os.kill(self.mpp["renamer"].pid, signal.SIGKILL)
                self.mpp["renamer"] = None
            except Exception as e:
                self.logger.info(str(e))
        # join verifier
        if self.mpp["verifier"]:
            self.logger.info("Waiting for par_verifier to complete")
            try:
                # clear queue
                while True:
                    try:
                        self.mp_parverify_inqueue.get_nowait()
                    except (queue.Empty, EOFError):
                        break
                self.mpp["verifier"].join()
            except Exception as e:
                self.logger.warning(str(e))
            self.mpp["verifier"] = None
        # if pw protected -> get pw and start unrarer
        # ispw = self.pwdb.db_nzb_get_ispw(nzbname)
        ispw = self.pwdb.exc("db_nzb_get_ispw", [nzbname], {})
        if ispw:
            get_pw_direct0 = False
            try:
                get_pw_direct0 = (self.cfg["OPTIONS"]["GET_PW_DIRECTLY"].lower() == "yes")
            except Exception as e:
                self.logger.warning(whoami() + str(e))
            # if self.pwdb.db_nzb_get_password(nzbname) == "N/A":
            if self.pwdb.exc("db_nzb_get_password", [nzbname], {}) == "N/A":
                self.logger.info("Trying to get password from file for NZB " + nzbname)
                # self.pwdb.db_msg_insert(nzbname, "trying to get password from pw file", "info")
                self.pwdb.exc("db_msg_insert", [nzbname, "trying to get password from pw file", "info"], {})
                pw = get_password(self.verifiedrar_dir, self.pw_file, nzbname, self.logger, get_pw_direct=get_pw_direct0)
                if pw:
                    self.logger.info("Found password " + pw + " for NZB " + nzbname)
                    # self.pwdb.db_msg_insert("Found password " + pw + " for NZB " + nzbname)
                    self.pwdb.exc("db_msg_insert", [nzbname, "found password " + pw, "info"], {})
                    # self.pwdb.db_nzb_set_password(nzbname, pw)
                    self.pwdb.exc("db_nzb_set_password", [nzbname, pw], {})
            else:
                # pw = self.pwdb.db_nzb_get_password(nzbname)
                pw = self.pwdb.exc("db_nzb_get_password", [nzbname], {})
            if not pw:
                self.logger.error("Cannot find password for NZB " + nzbname + "in postprocess, exiting ...")
                # self.pwdb.db_nzb_update_status(nzbname, -4)
                self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                return -1
            self.mpp_unrarer = mp.Process(target=partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                      nzbname, self.logger, pw, self.cfg, ))
            self.mpp_unrarer.start()
            self.mpp["unrarer"] = self.mpp_unrarer
            self.sighandler.mpp = self.mpp
        # finalverifierstate = (self.pwdb.db_nzb_get_verifystatus(nzbname) in [0, 2])
        finalverifierstate = (self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
        # join unrarer
        if self.mpp["unrarer"]:
            if finalverifierstate:
                self.logger.info("Waiting for unrar to complete")
                self.mpp["unrarer"].join()
            else:
                self.logger.info("Repair/unrar not possible, killing unrarer!")
                try:
                    os.kill(self.mpp["unrarer"].pid, signal.SIGKILL)
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
            self.mpp["unrarer"] = None
            self.sighandler.mpp = self.mpp
            self.logger.debug(whoami() + "Unrarer stopped!")
        # get status
        # finalverifierstate = (self.pwdb.db_nzb_get_verifystatus(nzbname) in [0, 2])
        finalverifierstate = (self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
        # finalnonrarstate = self.pwdb.db_allnonrarfiles_getstate(nzbname)
        finalnonrarstate = self.pwdb.exc("db_allnonrarfiles_getstate", [nzbname], {})
        # finalrarstate = (self.pwdb.db_nzb_get_unrarstatus(nzbname) in [0, 2])
        finalrarstate = (self.pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) in [0, 2])
        self.logger.info("Finalrarstate: " + str(finalrarstate) + " / Finalnonrarstate: " + str(finalnonrarstate))
        if finalrarstate and finalnonrarstate and finalverifierstate:
            # self.pwdb.db_msg_insert(nzbname, "postprocessing ok!", "success")
            self.pwdb.exc("db_msg_insert", [nzbname, "postprocessing ok!", "success"], {})
        else:
            # self.pwdb.db_nzb_update_status(nzbname, -4)
            self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            # self.pwdb.db_msg_insert(nzbname, "postprocessing failed!", "error")
            self.pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
            self.logger.info("postprocess of NZB " + nzbname + " failed!")
            return -1
        # copy to complete
        res0 = self.make_complete_dir()
        if not res0:
            # self.pwdb.db_nzb_update_status(nzbname, -4)
            self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            # self.pwdb.db_msg_insert(nzbname, "postprocessing failed!", "error")
            self.pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
            self.logger.info("Cannot create complete_dir for " + nzbname + ", exiting ...")
            # self.pwdb.db_msg_insert(nzbname, "postprocessing failed!", "error")
            self.pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
            return -1
        # move all non-rar/par2/par2vol files from renamed to complete
        for f00 in glob.glob(self.rename_dir + "*"):
            self.logger.debug(whoami() + "renamed_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
            if os.path.isdir(f00):
                self.logger.debug(f00 + "is a directory, skipping")
                continue
            f0 = f00.split("/")[-1]
            # file0type = self.pwdb.db_file_getftype_renamed(f0)
            file0type = self.pwdb.exc("db_file_getftype_renamed", [f0], {})
            self.logger.debug(whoami() + "Moving/deleting " + f0)
            if not file0type:
                gg = re.search(r"[0-9]+[.]rar[.]+[0-9]", f0, flags=re.IGNORECASE)
                if gg:
                    try:
                        os.remove(f00)
                        self.logger.debug(whoami() + "Removed rar.x file " + f0)
                    except Exception as e:
                        # self.pwdb.db_nzb_update_status(nzbname, -4)
                        self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                        self.logger.warning(whoami() + str(e) + ": cannot remove corrupt rar file!")
                else:    # if unknown file (not in db) move to complete anyway
                    try:
                        shutil.move(f00, self.complete_dir)
                        self.logger.debug(whoami() + "moved " + f00 + " to " + self.complete_dir)
                    except Exception as e:
                        self.logger.warning(whoami() + str(e) + ": cannot move unknown file to complete!")
                continue
            if file0type in ["rar", "par2", "par2vol"]:
                try:
                    os.remove(f00)
                    self.logger.debug(whoami() + "removed rar/par2 file " + f0)
                except Exception as e:
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    # self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.logger.warning(whoami() + str(e) + ": cannot remove rar/par2 file!")
            else:
                try:
                    shutil.move(f00, self.complete_dir)
                    self.logger.debug(whoami() + "moved non-rar/non-par2 file " + f0 + " to complete")
                except Exception as e:
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    # self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.logger.warning(whoami() + str(e) + ": cannot move non-rar/non-par2 file " + f00 + "!")
        # remove download_dir
        try:
            shutil.rmtree(self.download_dir)
        except Exception as e:
            self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            # self.pwdb.db_nzb_update_status(nzbname, -4)
            self.logger.warning(whoami() + str(e) + ": cannot remove download_dir!")
        # move content of unpack dir to complete
        self.logger.debug(whoami() + "moving unpack_dir to complete: " + self.unpack_dir)
        for f00 in glob.glob(self.unpack_dir + "*"):
            self.logger.debug(whoami() + "u1npack_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
            d0 = f00.split("/")[-1]
            self.logger.debug(whoami() + "Does " + self.complete_dir + d0 + " already exist?")
            if os.path.isfile(self.complete_dir + d0):
                try:
                    self.logger.debug(whoami() + self.complete_dir + d0 + " already exists, deleting!")
                    os.remove(self.complete_dir + d0)
                except Exception as e:
                    self.logger.debug(whoami() + f00 + " already exists but cannot delete")
                    # self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    break
            else:
                self.logger.debug(whoami() + self.complete_dir + d0 + " does not exist!")

            if not os.path.isdir(f00):
                try:
                    shutil.move(f00, self.complete_dir)
                except Exception as e:
                    # self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    self.logger.warning(str(e) + ": cannot move unrared file to complete dir!")
            else:
                if os.path.isdir(self.complete_dir + d0):
                    try:
                        shutil.rmtree(self.complete_dir + d0)
                    except Exception as e:
                        # self.pwdb.db_nzb_update_status(nzbname, -4)
                        self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                        self.logger.warning(str(e) + ": cannot remove unrared dir in complete!")
                try:
                    shutil.copytree(f00, self.complete_dir + d0)
                except Exception as e:
                    # self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    self.logger.warning(str(e) + ": cannot move non-rar/non-par2 file!")
        # remove unpack_dir
        # if self.pwdb.db_nzb_getstatus(nzbname) != -4:
        if self.pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
            try:
                shutil.rmtree(self.unpack_dir)
                shutil.rmtree(self.verifiedrar_dir)
            except Exception as e:
                # self.pwdb.db_nzb_update_status(nzbname, -4)
                self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                self.logger.warning(str(e) + ": cannot remove unpack_dir / verifiedrar_dir")
        # remove incomplete_dir
        # if self.pwdb.db_nzb_getstatus(nzbname) != -4:
        if self.pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
            try:
                shutil.rmtree(self.main_dir)
            except Exception as e:
                # self.pwdb.db_nzb_update_status(nzbname, -4)
                self.pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                self.logger.warning(str(e) + ": cannot remove incomplete_dir!")
        # finalize
        if self.pwdb.exc("db_nzb_getstatus", [nzbname], {}) == -4:
            # if self.pwdb.db_nzb_getstatus(nzbname) == -4:
            self.logger.info("Copy/Move of NZB " + nzbname + " failed!")
            # self.pwdb.db_msg_insert(nzbname, "postprocessing failed!", "error")
            self.pwdb.exc("db_msg_insert"[nzbname, "postprocessing failed!", "error"], {})
            self.guiconnector.set_data(downloaddata, self.ct.threads, self.ct.servers.server_config, "failed", self.serverconfig())
            return -1
        else:
            self.logger.info("Copy/Move of NZB " + nzbname + " success!")
            self.guiconnector.set_data(downloaddata, self.ct.threads, self.ct.servers.server_config, "success", self.serverconfig())
            self.pwdb.exc("db_nzb_update_status", [nzbname, 4], {})
            # self.pwdb.db_nzb_update_status(nzbname, 4)
            # self.pwdb.db_msg_insert(nzbname, "postprocessing success!", "success")
            self.pwdb.exc("db_msg_insert", [nzbname, "postprocessing success!", "success"], {})
            self.logger.info("Postprocess of NZB " + nzbname + " ok!")
            return 1

    # do sanitycheck on nzb (excluding articles for par2vols)
    def do_sanity_check(self, allfileslist, files, infolist, bytescount0):
        self.logger.info(whoami() + "performing sanity check")
        for t, _ in self.ct.threads:
            t.mode = "sanitycheck"
        sanity_injects = ["rar", "sfv", "nfo", "etc", "par2"]
        files, infolist, bytescount0, article_count = self.inject_articles(sanity_injects, allfileslist, files, infolist, bytescount0)
        artsize0 = self.articlequeue.qsize()
        self.logger.info(whoami() + "Checking sanity on " + str(artsize0) + " articles")
        self.articlequeue.join()
        nr_articles = 0
        nr_ok_articles = 0
        while True:
            try:
                resultarticle = self.resultqueue.get_nowait()
                self.resultqueue.task_done()
                nr_articles += 1
                _, _, _, _, _, _, _, status = resultarticle
                if status != "failed":
                    nr_ok_articles += 1
            except (queue.Empty, EOFError):
                break
        self.resultqueue.join()
        if nr_articles == 0:
            a_health = 0
        else:
            a_health = nr_ok_articles / (nr_articles)
        self.logger.info(whoami() + "article health: {0:.4f}".format(a_health * 100) + "%")
        for t, _ in self.ct.threads:
            t.mode = "download"
        return a_health

    # main download routine
    def download(self, allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, servers_shut_down, resqlist):

        self.logger.info(whoami() + "downloading " + nzbname)
        # self.pwdb.db_msg_insert(nzbname, "initializing download", "info")
        self.pwdb.exc("db_msg_insert", [nzbname, "initializing download", "info"], {})

        self.resqlist = resqlist

        # init variables
        self.logger.debug(whoami() + "download: init variables")
        self.mpp_renamer = None
        self.mpp_decoder = None
        article_failed = 0
        inject_set0 = []
        avgmiblist = []
        inject_set0 = ["par2"]             # par2 first!!
        files = {}
        infolist = {}
        loadpar2vols = False
        availmem0 = psutil.virtual_memory()[0] - psutil.virtual_memory()[1]
        bytescount0 = 0
        article_health = 1
        self.ct.reset_timestamps()
        # if self.pwdb.db_nzb_getstatus(nzbname) > 2:
        if self.pwdb.exc("db_nzb_getstatus", [nzbname], {}) > 2:
            self.logger.info(nzbname + "- download complete!")
            return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                              overall_size, already_downloaded_size)), "dl_finished", self.main_dir
        # self.pwdb.db_nzb_update_status(nzbname, 2)    # status "downloading"
        self.pwdb.exc("db_nzb_update_status", [nzbname, 2], {})    # status "downloading"

        if filetypecounter["par2vol"]["max"] > 0:
            self.contains_par_files = True

        # which set of filetypes should I download
        self.logger.debug(whoami() + "download: define inject set")
        if filetypecounter["par2"]["max"] > 0 and filetypecounter["par2"]["max"] > filetypecounter["par2"]["counter"]:
            inject_set0 = ["par2"]
        # elif self.pwdb.db_nzb_loadpar2vols(nzbname):
        elif self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            inject_set0 = ["etc", "par2vol", "rar", "sfv", "nfo"]
            loadpar2vols = True
        else:
            inject_set0 = ["etc", "sfv", "nfo", "rar"]
        self.logger.info(whoami() + "Overall_Size: " + str(overall_size) + ", incl. par2vols: " + str(overall_size_wparvol))

        # make dirs
        self.logger.debug(whoami() + "creating dirs")
        self.make_dirs(nzbname)
        self.sighandler.main_dir = self.main_dir

        # start decoder mpp
        self.logger.debug(whoami() + "starting decoder process ...")
        self.mpp_decoder = mp.Process(target=decode_articles, args=(self.mp_work_queue, self.cfg, self.logger, ))
        self.mpp_decoder.start()
        self.mpp["decoder"] = self.mpp_decoder

        # start renamer
        self.logger.debug(whoami() + "starting renamer process ...")
        self.mpp_renamer = mp.Process(target=renamer, args=(self.download_dir, self.rename_dir, self.cfg, self.mp_result_queue, self.logger, ))
        self.mpp_renamer.start()
        self.mpp["renamer"] = self.mpp_renamer
        self.sighandler.mpp = self.mpp
        self.sighandler.nzbname = nzbname

        if servers_shut_down:
            # start download threads
            self.logger.debug(whoami() + "starting download threads")
            if not self.ct.threads:
                self.ct.start_threads()
                self.sighandler.servers = self.ct.servers

        bytescount0 = self.getbytescount(allfileslist)

        # sanity check
        inject_set_sanity = []
        # if self.cfg["OPTIONS"]["SANITY_CHECK"].lower() == "yes" and not self.pwdb.db_nzb_loadpar2vols(nzbname):
        if self.cfg["OPTIONS"]["SANITY_CHECK"].lower() == "yes" and not self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            sanity0 = self.do_sanity_check(allfileslist, files, infolist, bytescount0)
            if sanity0 < 1:
                # self.pwdb.db_nzb_update_loadpar2vols(nzbname, True)
                self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                overall_size = overall_size_wparvol
                self.logger.info(whoami() + "queuing par2vols")
                inject_set_sanity = ["par2vol"]

        # inject articles and GO!
        files, infolist, bytescount0, article_count = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)

        # inject par2vols because of sanity check
        if inject_set_sanity:
            files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set_sanity, allfileslist, files, infolist, bytescount0)
            bytescount0 += bytescount00
            article_count += article_count0

        getnextnzb = False
        article_failed = 0

        # reset bytesdownloaded
        self.ct.reset_timestamps()

        # download loop until articles downloaded
        oldrarcounter = 0
        # self.pwdb.db_msg_insert(nzbname, "downloading", "info")
        self.pwdb.exc("db_msg_insert", [nzbname, "downloading", "info"], {})
        while True:

            # check if dl_stopped or nzbs_reordered signal received from gui
            return_reason = None
            with self.guiconnector.lock:
                if not self.guiconnector.dl_running:
                    return_reason = "dl_stopped"
                if self.guiconnector.has_first_nzb_changed():
                    if not self.guiconnector.has_nzb_been_deleted():
                        self.logger.debug(whoami() + "NZBs have been reorderd, exiting download loop")
                        return_reason = "nzbs_reordered"
                    else:
                        self.logger.debug(whoami() + "NZBs have been deleted, exiting download loop")
                        return_reason = "nzbs_deleted"
            if return_reason:
                return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                  overall_size, already_downloaded_size)), return_reason, self.main_dir

            # if dl is finished
            if getnextnzb:
                self.logger.info(nzbname + "- download complete!")
                # self.pwdb.db_nzb_update_status(nzbname, 3)
                self.pwdb.exc("db_nzb_update_status", [nzbname, 3], {})
                break

            # get mp_result_queue (renamer.py)
            while True:
                try:
                    filename, full_filename, filetype, old_filename, old_filetype = self.mp_result_queue.get_nowait()
                    # self.pwdb.db_msg_insert(nzbname, "downloaded " + filename, "info")
                    self.pwdb.exc("db_msg_insert", [nzbname, "downloaded " + filename, "info"], {})
                    # have files been renamed ?
                    if old_filename != filename or old_filetype != filetype:
                        self.logger.info(whoami() + old_filename + "/" + old_filetype + " changed to " + filename + " / " + filetype)
                        # update filetypecounter
                        filetypecounter[old_filetype]["filelist"].remove(old_filename)
                        filetypecounter[filetype]["filelist"].append(filename)
                        filetypecounter[old_filetype]["max"] -= 1
                        filetypecounter[filetype]["counter"] += 1
                        filetypecounter[filetype]["max"] += 1
                        filetypecounter[filetype]["loadedfiles"].append((filename, full_filename))
                        # update allfileslist
                        for i, o_lists in enumerate(allfileslist):
                            o_orig_name, o_age, o_type, o_nr_articles = o_lists[0]
                            if o_orig_name == old_filename:
                                allfileslist[i][0] = (filename, o_age, o_type, o_nr_articles)
                    else:
                        self.logger.debug(whoami() + "moved " + filename + " to renamed dir")
                        filetypecounter[filetype]["counter"] += 1
                        filetypecounter[filetype]["loadedfiles"].append((filename, full_filename))
                    if (filetype == "par2" or filetype == "par2vol") and not p2:
                        p2 = Par2File(full_filename)
                        self.logger.info(whoami() + "found first par2 file")
                    if inject_set0 == ["par2"] and (filetype == "par2" or filetypecounter["par2"]["max"] == 0):
                        self.logger.debug(whoami() + "injecting rars etc.")
                        inject_set0 = ["etc", "sfv", "nfo", "rar"]
                        files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                        bytescount0 += bytescount00
                        article_count += article_count0
                except (queue.Empty, EOFError):
                    break

            # get mp_parverify_inqueue
            if not loadpar2vols:
                while True:
                    try:
                        loadpar2vols = self.mp_parverify_inqueue.get_nowait()
                    except (queue.Empty, EOFError):
                        break
                if loadpar2vols:
                    # self.pwdb.db_nzb_update_loadpar2vols(nzbname, True)
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    overall_size = overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                    bytescount0 += bytescount00
                    article_count += article_count0

            # check if unrarer is dead due to wrong rar on start
            # if self.mpp["unrarer"] and self.pwdb.db_nzb_get_unrarstatus(nzbname) == -2:
            if self.mpp["unrarer"] and self.pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) == -2:
                self.mpp["unrarer"] = None
                self.sighandler.mpp = self.mpp

            # if self.mpp["verifier"] and self.pwdb.db_nzb_get_verifystatus(nzbname) == 2:
            if self.mpp["verifier"] and self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) == 2:
                self.mpp["verifier"] = None
                self.sighandler.mpp = self.mpp

            if not self.mpp["unrarer"] and filetypecounter["rar"]["counter"] > oldrarcounter and not self.pwdb.exc("db_nzb_get_ispw", [nzbname], {}):
                # testing if pw protected
                rf = [rf0 for _, _, rf0 in os.walk(self.verifiedrar_dir) if rf0]
                # if no rar files in verified_rardir: skip as we cannot test for password
                if rf:
                    oldrarcounter = filetypecounter["rar"]["counter"]
                    self.logger.debug(whoami() + ": first/new verified rar file appeared, testing if pw protected")
                    is_pwp = is_rar_password_protected(self.verifiedrar_dir, self.logger)
                    if is_pwp in [0, -2]:
                        self.logger.warning(whoami() + "cannot test rar if pw protected, something is wrong: " + str(is_pwp) + ", exiting ...")
                        # self.pwdb.db_nzb_update_status(nzbname, -2)  # status download failed
                        self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})  # status download failed
                        self.guiconnector.set_data((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname,
                                                    article_health, overall_size, already_downloaded_size), self.ct.threads,
                                                   self.ct.servers.server_config, "failed", self.serverconfig())
                        return_reason = "dl_failed"
                        # self.pwdb.db_msg_insert(nzbname, "download failed due to pw test not possible", "error")
                        self.pwdb.exc("db_msg_insert", [nzbname, "download failed due to pw test not possible", "error"], {})
                        return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                          overall_size, already_downloaded_size)), return_reason, self.main_dir
                    if is_pwp == 1:
                        # if pw protected -> postpone password test + unrar
                        # self.pwdb.db_nzb_set_ispw(nzbname, True)
                        self.pwdb.exc("db_nzb_set_ispw", [nzbname, True], {})
                        # self.pwdb.db_msg_insert(nzbname, "rar archive is password protected", "warning")
                        self.pwdb.exc("db_msg_insert", [nzbname, "rar archive is password protected", "warning"], {})
                        self.logger.info(whoami() + "rar archive is pw protected, postponing unrar to postprocess ...")
                    elif is_pwp == -1:
                        # if not pw protected -> normal unrar
                        self.logger.info(whoami() + "rar archive is not pw protected, starting unrarer ...")
                        # self.pwdb.db_nzb_set_ispw(nzbname, False)
                        self.pwdb.exc("db_nzb_set_ispw", [nzbname, False], {})
                        self.mpp_unrarer = mp.Process(target=partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                                  nzbname, self.logger, None, self.cfg, ))
                        self.mpp_unrarer.start()
                        self.mpp["unrarer"] = self.mpp_unrarer
                        self.sighandler.mpp = self.mpp
                    elif is_pwp == -3:
                        self.logger.info(whoami() + ": cannot check for pw protection as first rar not present yet")
                else:
                    self.logger.debug(whoami() + "no rars in verified_rardir yet, cannot test for pw / start unrarer yet!")

            # if par2 available start par2verifier, else just copy rars unchecked!
            if not self.mpp["verifier"]:
                # todo: check if all rars are verified
                # all_rars_are_verified, _ = self.pwdb.db_only_verified_rars(nzbname)
                all_rars_are_verified, _ = self.pwdb.exc("db_only_verified_rars", [nzbname], {})
                if not all_rars_are_verified:
                    pvmode = None
                    if p2:
                        pvmode = "verify"
                    elif not p2 and filetypecounter["par2"]["max"] == 0:
                        pvmode = "copy"
                    if pvmode:
                        self.logger.debug(whoami() + "starting rar_verifier process (mode=" + pvmode + ")for NZB " + nzbname)
                        self.mpp_verifier = mp.Process(target=par_verifier, args=(self.mp_parverify_inqueue, self.rename_dir, self.verifiedrar_dir,
                                                                                  self.main_dir, self.logger, nzbname, pvmode, self.cfg, ))
                        self.mpp_verifier.start()
                        self.mpp["verifier"] = self.mpp_verifier
                        self.sighandler.mpp = self.mpp

            # read resultqueue + decode via mp
            newresult, avgmiblist, infolist, files, failed = self.process_resultqueue(avgmiblist, infolist, files)
            article_failed += failed
            if article_count != 0:
                article_health = 1 - article_failed / article_count
            else:
                article_health = 0
            self.article_health = article_health
            if failed != 0:
                self.logger.warning(whoami() + str(failed) + " articles failed, article_count: " + str(article_count) + ", health: " + str(article_health))
                # if too many missing articles: exit download
                if (article_health < self.crit_art_health_wo_par and filetypecounter["par2vol"]["max"] == 0) \
                   or (filetypecounter["parvols"]["max"] > 0 and article_health <= self.crit_art_health_w_par):
                    self.logger.info(whoami() + "articles missing and cannot repair, exiting download")
                    # self.pwdb.db_nzb_update_status(nzbname, -2)
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                    self.guiconnector.set_data((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname,
                                                article_health, overall_size, already_downloaded_size), self.ct.threads,
                                               self.ct.servers.server_config, "failed", self.serverconfig())
                    # self.pwdb.db_msg_insert(nzbname, "critical health threashold exceeded", "error")
                    self.pwdb.exc("db_msg_insert", [nzbname, "critical health threashold exceeded", "error"], {})
                    return_reason = "dl_failed"
                    return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                      overall_size, already_downloaded_size)), return_reason, self.main_dir
                if not loadpar2vols and filetypecounter["parvols"]["max"] > 0 and article_health > 0.95:
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                    bytescount0 += bytescount00
                    article_count += article_count0
                    overall_size = overall_size_wparvol
                    loadpar2vols = True

            try:
                self.guiconnector.set_data((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname,
                                            article_health, overall_size, already_downloaded_size), self.ct.threads,
                                           self.ct.servers.server_config, "downloading", self.serverconfig())
            except Exception as e:
                self.logger.warning(whoami() + "set_data error " + str(e))

            # check if all files are downloaded
            getnextnzb = True
            for filetype, item in filetypecounter.items():
                if filetype == "par2vol" and not loadpar2vols:
                    continue
                if filetypecounter[filetype]["counter"] < filetypecounter[filetype]["max"]:
                    getnextnzb = False
                    break

            # if all files are downloaded and still articles in queue --> inconsistency, exit!
            if getnextnzb and not self.all_queues_are_empty:
                # self.pwdb.db_msg_insert(nzbname, "inconsistency in download queue", "error")
                self.pwdb.exc("db_msg_insert", [nzbname, "inconsistency in download queue", "error"], {})
                # self.pwdb.db_nzb_update_status(nzbname, -2)
                self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                self.logger.warning(whoami() + ": records say dl is done, but still some articles in queue, exiting ...")
                self.guiconnector.set_data((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname,
                                            article_health, overall_size, already_downloaded_size), self.ct.threads,
                                           self.ct.servers.server_config, "failed", self.serverconfig())
                return_reason = "dl_failed"
                return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                  overall_size, already_downloaded_size)), return_reason, self.main_dir

            # check if > 25% of connections are down
            self.connection_health = self.connection_thread_health()
            self.guiconnector.set_health(self.article_health, self.connection_health)
            if self.connection_health < 0.65:
                # self.logger.debug(whoami() + ">>>> " + str(self.connection_thread_health()))
                self.logger.info(whoami() + "connections are unstable, restarting")
                # self.pwdb.db_msg_insert(nzbname, "connections are unstable, restarting", "warning")
                self.pwdb.exc("db_msg_insert", [nzbname, "connections are unstable, restarting", "warning"], {})
                return_reason = "connection_restart"
                return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                  overall_size, already_downloaded_size)), return_reason, self.main_dir
                # self.restart_all_threads()

            time.sleep(0.3)

        return_reason = "dl_finished"
        # self.pwdb.db_msg_insert(nzbname, "download complete", "success")
        self.pwdb.exc("db_msg_insert", [nzbname, "download complete", "success"], {})
        return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                          overall_size, already_downloaded_size)), return_reason, self.main_dir

    def get_level_servers(self, retention):
        le_serv0 = []
        for level, serverlist in self.ct.level_servers.items():
            level_servers = serverlist
            le_dic = {}
            for le in level_servers:
                _, _, _, _, _, _, _, _, age = self.ct.servers.get_single_server_config(le)
                le_dic[le] = age
            les = [le for le in level_servers if le_dic[le] > retention * 0.9]
            le_serv0.append(les)
        return le_serv0


def get_next_nzb(pwdb, dirs, ct, logger):
    # waiting for nzb_parser to insert all nzbs in nzbdir into db
    try:
        nzbs_in_nzbdirs = [n.split("/")[-1] for n in glob.glob(dirs["nzb"] + "*")]
        if nzbs_in_nzbdirs:
            while True:
                all_nzbs_exist_in_db = True
                for n in nzbs_in_nzbdirs:
                    # if not pwdb.db_nzb_exists(n):
                    if not pwdb.exc("db_nzb_exists", [n], {}):
                        all_nzbs_exist_in_db = False
                        break
                if all_nzbs_exist_in_db:
                    break
                time.sleep(1)
    except Exception as e:
        logger.error(whoami() + str(e))

    logger.debug(whoami() + "looking for new NZBs ...")
    try:
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
            = make_allfilelist_inotify(pwdb, dirs, logger, -1)
    except Exception as e:
        logger.warning(whoami() + str(e))
    # poll for 30 sec if no nzb immediately found
    if not nzbname:
        logger.debug(whoami() + "polling for 30 sec. for new NZB before closing connections if alive ...")
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
            = make_allfilelist_inotify(pwdb, dirs, logger, 30 * 1000)
        if not nzbname:
            if ct.threads:
                # if no success: close all connections and poll blocking
                logger.warning("Idle time > 30 sec, closing all server connections")
                for t, _ in ct.threads:
                    t.stop()
                    t.join()
                ct.servers.close_all_connections()
                ct.threads = []
                del ct.servers
            logger.debug(whoami() + "polling for new NZBs now in blocking mode!")
            try:
                allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
                    = make_allfilelist_inotify(pwdb, dirs, logger, None)
            except Exception as e:
                logger.warning(whoami() + str(e))
    pwdb.exc("send_sorted_nzbs_to_guiconnector", [], {})
    return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist


def make_allfilelist_inotify(pwdb, dirs, logger, timeout0):
    # immediatley get allfileslist
    if timeout0 and timeout0 <= -1:
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
           = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
#            = pwdb.make_allfilelist(dirs["incomplete"], dirs["nzb"])
        if nzbname:
            logger.debug(whoami() + "inotify: no timeout, got nzb " + nzbname + " immediately!")
            return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist
        else:
            return None, None, None, None, None, None, None, None
    # setup inotify
    logger.debug(whoami() + "Setting up inotify for timeout=" + str(timeout0))
    t0 = time.time()
    if not timeout0:
        delay0 = 5
    else:
        delay0 = 1
    while True:
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
            = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
        if nzbname:
            logger.debug(whoami() + "new nzb found in db, queuing ...")
            return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist
        if timeout0:
            if time.time() - t0 > timeout0 / 1000:
                break
        time.sleep(delay0)
    return None, None, None, None, None, None, None, None

    '''pwdb_inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    wd = pwdb_inotify.add_watch(dirs["main"], watch_flags)
    t0 = time.time()
    timeout00 = timeout0
    while True:
        for event in pwdb_inotify.read(timeout=timeout00):
            logger.debug(whoami() + "got notify event on " + str(event.name))
            if event.name == u"ginzibix.db":
                logger.debug(whoami() + "database updated, now checking for nzbname & data")
                allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
                    = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
                # = pwdb.make_allfilelist(dirs["incomplete"], dirs["nzb"])
                if nzbname:
                    logger.debug(whoami() + "new nzb found in db, queuing ...")
                    pwdb_inotify.rm_watch(wd)
                    return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist
                else:
                    logger.debug(whoami() + "no new nzb found in db, continuing polling ...")
        # if timeout == None: again blocking, else subtract already spent timeout
        if timeout00:
            timeout00 = timeout00 - (time.time() - t0) * 1000
            t0 = time.time()
            if timeout00 <= 0:
                pwdb_inotify.rm_watch(wd)
                return None, None, None, None, None, None, None, None'''


# this class deals on a meta-level with usenet connections
class ConnectionThreads:
    def __init__(self, cfg, articlequeue, resultqueue, logger):
        self.cfg = cfg
        self.logger = logger
        self.threads = []
        self.lock = threading.Lock()
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.servers = None

    def init_servers(self):
        self.servers = Servers(self.cfg, self.logger)
        self.level_servers = self.servers.level_servers
        self.all_connections = self.servers.all_connections

    def start_threads(self):
        if not self.threads:
            self.logger.debug(whoami() + "starting download threads")
            self.init_servers()
            for sn, scon, _, _ in self.all_connections:
                t = ConnectionWorker(self.lock, (sn, scon), self.articlequeue, self.resultqueue, self.servers, self.logger)
                self.threads.append((t, time.time()))
                t.start()
        else:
            self.logger.debug(whoami() + "threads already started")

    def reset_timestamps(self):
        for t, _ in self.threads:
            t.last_timestamp = time.time()

    def reset_timestamps_bdl(self):
        if self.threads:
            for t, _ in self.threads:
                t.bytesdownloaded = 0
                t.last_timestamp = 0
                t.bandwidth_bytes = 0
                t.bandwidth_lasttt = 0


def write_resultqueue_to_file(resultqueue, maindir, logger):
    logger.debug(whoami() + "reading resultqueue and writing to " + maindir)
    resqlist = []
    bytes_in_resultqueue = 0
    while True:
        try:
            res = resultqueue.get_nowait()
            # (fn_r, age_r, ft_r, nr_art_r, art_nr_r, art_name_r, download_server_r, inf0_r, False)
            _, _, _, _, _, art_name, _, inf0, _ = res
            art_size = 0
            if inf0 != "failed":
                art_size = sum(len(i) for i in inf0)
            bytes_in_resultqueue += art_size
            resultqueue.task_done()
            resqlist.append(res)
        except (queue.Empty, EOFError):
            break
        except Exception as e:
            logger.info(whoami() + ": " + str(e))
    fn = maindir + "resqueue.gzbx"
    if resqlist:
        try:
            with open(fn, "wb") as fp:
                dill.dump(resqlist, fp)
        except Exception as e:
            logger.warning(whoami() + str(e) + ": cannot write resqueue.gzbx")
    else:
        try:
            os.remove(fn)
        except Exception as e:
            logger.warning(whoami() + str(e) + ": cannot remove resqueue.gzbx")
    return bytes_in_resultqueue


def clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger):
    # join all queues
    logger.debug(whoami() + "clearing articlequeue")
    # 1. clear articlequeue
    while True:
        try:
            articlequeue.get_nowait()
            articlequeue.task_done()
        except (queue.Empty, EOFError):
            break
    articlequeue.join()
    # 2. wait for all remaining articles to be downloaded
    logger.debug(whoami() + "waiting for all remaining articles to be downloaded")
    dl_not_done_yet = True
    while dl_not_done_yet:
        dl_not_done_yet = False
        for t, _ in dl.ct.threads:
            if not t.is_download_done():
                dl_not_done_yet = True
                break
        if dl_not_done_yet:
            time.sleep(0.2)
    # 3. stop article_decoder
    mpid = None
    try:
        if dl.mpp["decoder"]:
            mpid = dl.mpp["decoder"].pid
        if mpid:
            logger.warning("terminating decoder")
            try:
                os.kill(dl.mpp["decoder"].pid, signal.SIGKILL)
                dl.mpp["decoder"].join()
                dl.mpp["decoder"] = None
                dl.sighandler.mpp = dl.mpp
            except Exception as e:
                logger.debug(whoami() + str(e))
    except Exception as e:
        logger.debug(whoami() + ": " + str(e))
    # 4. clear mp_work_queue
    logger.debug(whoami() + "clearing mp_work_queue")
    while True:
        try:
            mp_work_queue.get_nowait()
        except (queue.Empty, EOFError):
            break
    # 5. save resultqueue
    bytes_in_resultqueue = write_resultqueue_to_file(resultqueue, maindir, logger)
    # pwdb.db_nzb_set_bytes_in_resultqueue(nzbname, bytes_in_resultqueue)
    pwdb.exc("db_nzb_set_bytes_in_resultqueue", [nzbname, bytes_in_resultqueue], {})
    # 6. stop unrarer
    mpid = None
    try:
        if dl.mpp["unrarer"]:
            mpid = dl.mpp["unrarer"].pid
        if mpid:
            # if self.mpp["unrarer"].pid:
            logger.warning("terminating unrarer")
            try:
                os.kill(mpid, signal.SIGKILL)
                dl.mpp["unrarer"].join()
                dl.mpp["unrarer"] = None
                dl.sighandler.mpp = dl.mpp
            except Exception as e:
                logger.debug(whoami() + str(e))
    except Exception as e:
        logger.debug(whoami() + ": " + str(e))
    # 7. stop rar_verifier
    mpid = None
    try:
        if dl.mpp["verifier"]:
            mpid = dl.mpp["verifier"].pid
        if mpid:
            logger.warning("terminating rar_verifier")
            try:
                os.kill(dl.mpp["verifier"].pid, signal.SIGKILL)
                dl.mpp["verifier"].join()
                dl.mpp["verifier"] = None
                dl.sighandler.mpp = dl.mpp
            except Exception as e:
                logger.debug(whoami() + str(e))
    except Exception as e:
        logger.debug(whoami() + ": " + str(e))
    # 8. stop mpp_renamer
    mpid = None
    try:
        if dl.mpp["renamer"]:
            mpid = dl.mpp["renamer"].pid
        if mpid:
            logger.warning("terminating renamer")
            try:
                os.kill(dl.mpp["renamer"].pid, signal.SIGTERM)
                dl.mpp["renamer"].join()
                dl.mpp["renamer"].join()
                dl.mpp["renamer"] = None
                dl.sighandler.mpp = dl.mpp
            except Exception as e:
                logger.debug(str(e))
    except Exception as e:
        logger.debug(whoami() + ": " + str(e))
    return


# main loop for ginzibix downloader
def ginzi_main(cfg, dirs, subdirs, logger):

    pwdb = PWDBSender(cfg)

    mp_work_queue = mp.Queue()
    articlequeue = queue.LifoQueue()
    resultqueue = queue.Queue()
    ct = ConnectionThreads(cfg, articlequeue, resultqueue, logger)

    # init sighandler
    logger.debug(whoami() + "initializing sighandler")
    mpp = {"nzbparser": None, "decoder": None, "unrarer": None, "renamer": None, "verifier": None}
    sh = SigHandler_Main(mpp, ct, mp_work_queue, resultqueue, articlequeue, pwdb, logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    # start nzb parser mpp
    logger.debug(whoami() + "starting nzbparser process ...")
    mpp_nzbparser = mp.Process(target=ParseNZB, args=(cfg, dirs["nzb"], logger, ))
    mpp_nzbparser.start()
    mpp["nzbparser"] = mpp_nzbparser

    sh.mpp = mpp

    try:
        lock = threading.Lock()
        guiconnector = GUI_Connector(lock, dirs, logger, cfg)
        guiconnector.start()
        logger.debug(whoami() + "guiconnector process started!")
    except Exception as e:
        logger.warning(whoami() + str(e))

    dl = Downloader(cfg, dirs, ct, mp_work_queue, sh, mpp, guiconnector, logger)
    servers_shut_down = True

    while True:
        sh.nzbname = None
        sh.main_dir = None
        guiconnector.set_health(0, 0)
        with lock:
            if not guiconnector.dl_running:
                time.sleep(1)
                continue
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2, resqlist \
            = get_next_nzb(pwdb, dirs, ct, logger)
        ct.reset_timestamps_bdl()
        if not nzbname:
            break
        logger.info(whoami() + "got next NZB: " + str(nzbname))

        nzbname, downloaddata, return_reason, maindir = dl.download(allfileslist, filetypecounter, nzbname, overall_size,
                                                                    overall_size_wparvol, already_downloaded_size, p2, servers_shut_down, resqlist)
        stat0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
        # stat0 = pwdb.db_nzb_getstatus(nzbname)

        if stat0 == 2:
            if return_reason == "connection_restart":
                logger.debug(whoami() + "restarting connections")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger)
                dl.restart_all_threads()
                continue
            elif return_reason == "nzbs_reordered":
                logger.debug(whoami() + "NZBs have been reordered")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger)
                continue
            elif return_reason == "nzbs_deleted":
                logger.debug(whoami() + "NZBs have been deleted")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger)
                deleted_nzb_name0 = guiconnector.has_nzb_been_deleted(delete=True)
                remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger)
                continue
            elif return_reason == "dl_stopped":
                logger.debug(whoami() + "download paused")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger)
                # idle until start or nzbs_reordered signal comes from gtkgui
                idlestart = time.time()
                servers_shut_down = False
                dl.ct.reset_timestamps_bdl()
                while True:
                    dobreak = False
                    with guiconnector.lock:
                        if guiconnector.dl_running:
                            dobreak = True
                        if guiconnector.has_first_nzb_changed():
                            deleted_nzb_name0 = guiconnector.has_nzb_been_deleted(delete=True)
                            if deleted_nzb_name0:
                                remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger)
                            # pwdb.send_sorted_nzbs_to_guiconnector()
                            pwdb.exc("send_sorted_nzbs_to_guiconnector", [], {})
                    if dobreak:
                        break
                    time.sleep(1)
                    # after 2min idle -> stop threads
                    if not servers_shut_down and time.time() - idlestart > 2 * 60:
                        # stop all threads
                        logger.debug(whoami() + "stopping all threads")
                        for t, _ in dl.ct.threads:
                            t.stop()
                            t.join()
                        dl.ct.threads = []
                        dl.ct.servers.close_all_connections()
                        del dl.ct.servers
                        servers_shut_down = True
                    time.sleep(1)
                continue

        # if download success, postprocess
        elif stat0 == 3:
            guiconnector.set_health(0, 0)
            logger.info(whoami() + "download success, postprocessing NZB " + nzbname)
            dl.postprocess_nzb(nzbname, downloaddata)
            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger)
            # stat0_0 = pwdb.db_nzb_getstatus(nzbname)
            stat0_0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
            if stat0_0 == 4:
                pwdb.exc("db_msg_insert", [nzbname, "downloaded and postprocessed successfully!", "success"], {})
                # pwdb.db_msg_insert(nzbname, "downloaded and postprocessed successfully!", "success")
            else:
                pwdb.exc("db_msg_insert", [nzbname, "download and/or postprocessing failed!", "error"], {})
                # pwdb.db_msg_insert(nzbname, "download and/or postprocessing failed!", "error")
            pwdb.exc("send_sorted_nzbs_to_guiconnector", [], {})
            # pwdb.send_sorted_nzbs_to_guiconnector()
            logger.info(whoami() + nzbname + " finished with status " + str(stat0_0))
        elif stat0 == -2:
            guiconnector.set_health(0, 0)
            pwdb.exc("db_msg_insert", [nzbname, "download failed!", "error"], {})
            logger.info(whoami() + "download failed for NZB " + nzbname)
            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, maindir, logger)
            pwdb.exc("send_sorted_nzbs_to_guiconnector", [], {})
            # pwdb.send_sorted_nzbs_to_guiconnector()
