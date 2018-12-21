#!/home/stephan/.virtualenvs/nntp/bin/python

from threading import Thread
import time
import sys
import os
import queue
import signal
import multiprocessing as mp
import psutil
import re
import threading
import shutil
import pickle
from .renamer import renamer
from .par_verifier import par_verifier
from .par2lib import Par2File
from .partial_unrar import partial_unrar
from .nzb_parser import ParseNZB
from .article_decoder import decode_articles
from .passworded_rars import is_rar_password_protected
from .connections import ConnectionThreads
from .aux import PWDBSender
from .guiconnector import GUI_Connector
from .postprocessor import postprocess_nzb, postproc_pause, postproc_resume

import inspect

lpref = __name__.split("lib.")[-1] + " - "


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


_ftypes = ["etc", "rar", "sfv", "par2", "par2vol"]


class SigHandler_Main:

    def __init__(self, mpp, ct, mp_work_queue, resultqueue, articlequeue, pwdb, logger):
        self.logger = logger
        self.ct = ct
        self.mpp = mpp
        self.mp_work_queue = mp_work_queue
        self.resultqueue = resultqueue
        self.articlequeue = articlequeue
        self.dirs = None
        self.nzbname = None
        self.pwdb = pwdb
        self.mon = None

    def shutdown(self):
        self.logger.info(whoami() + "starting shutdown sequence ...")
        if self.mon:
            self.logger.debug(whoami() + "terminating monitor ...")
            self.mon.stop()
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
        '''self.logger.debug(whoami() + "waiting for all remaining articles to be downloaded")
        dl_not_done_yet = True
        while dl_not_done_yet:
            dl_not_done_yet = False
            for t, _ in self.ct.threads:
                if not t.is_download_done():
                    dl_not_done_yet = True
                    break
            if dl_not_done_yet:
                time.sleep(0.2)'''
        # 3. stop decoder
        mpid = None
        try:
            if self.mpp["decoder"]:
                mpid = self.mpp["decoder"].pid
            if mpid:
                self.logger.debug(whoami() + "terminating decoder")
                try:
                    os.kill(self.mpp["decoder"].pid, signal.SIGTERM)
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
        if self.dirs and self.nzbname:
            self.logger.debug(whoami() + "writing resultqueue to .gzbx file")
            time.sleep(0.5)
            bytes_in_resultqueue = write_resultqueue_to_file(self.resultqueue, self.dirs, self.pwdb, self.nzbname, self.logger)
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
                    os.kill(self.mpp["unrarer"].pid, signal.SIGTERM)
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
                    os.kill(self.mpp["verifier"].pid, signal.SIGTERM)
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
                    os.kill(self.mpp["renamer"].pid, signal.SIGTERM)
                    self.mpp["renamer"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 9. stop nzbparser
        try:
            mpid = None
            if self.mpp["nzbparser"].is_alive():
                mpid = self.mpp["nzbparser"].pid
                self.logger.debug(whoami() + "terminating nzb_parser")
                try:
                    os.kill(self.mpp["nzbparser"].pid, signal.SIGTERM)
                    self.mpp["nzbparser"].join()
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
        except Exception as e:
            self.logger.debug(whoami() + str(e))
        # 10. threads + servers
        self.ct.stop_threads()

        self.logger.info(whoami() + "exited!")
        sys.exit()

    def sighandler(self, a, b):
        self.shutdown()


# Handles download of a NZB file
class Downloader(Thread):
    def __init__(self, cfg, dirs, ct, mp_work_queue, sighandler, mpp, guiconnector, pipes, renamer_result_queue, mp_events,
                 nzbname, logger):
        Thread.__init__(self)
        self.daemon = True
        self.lock = threading.Lock()
        self.nzbname = nzbname
        self.event_unrareridle = mp_events["unrarer"]
        self.event_verifieridle = mp_events["verifier"]
        self.cfg = cfg
        self.pipes = pipes
        self.pwdb = PWDBSender()
        self.articlequeue = ct.articlequeue
        self.resultqueue = ct.resultqueue
        self.mp_work_queue = mp_work_queue
        self.renamer_result_queue = renamer_result_queue
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
        self.results = None
        self.read_cfg()
        self.stopped = False
        self.paused = False

        if self.pw_file:
            try:
                self.logger.debug(whoami() + "as a first test, open password file")
                with open(self.pw_file, "r") as f0:
                    f0.readlines()
                self.logger.info(whoami() + "password file is available")
            except Exception as e:
                self.logger.warning(whoami() + str(e) + ": cannot open pw file, setting to None")
                self.pw_file = None

    def stop(self):
        with self.lock:
            self.stopped = True

    def pause(self):
        with self.lock:
            self.paused = True

    def resume(self):
        with self.lock:
            self.paused = False

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

    def serverhealth(self):
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

    def inject_articles(self, ftypes, filelist, files0, infolist0, bytescount0_0, filetypecounter):
        # generate all articles and files
        files = files0
        infolist = infolist0
        bytescount0 = bytescount0_0
        article_count = 0
        for f in ftypes:
            for j, file_articles in enumerate(reversed(filelist)):
                # iterate over all articles in file
                filename, age, filetype, nr_articles = file_articles[0]
                filestatus = self.pwdb.exc("db_file_getstatus", [filename], {})
                # reconcile filetypecounter with db
                if filetype == f:
                    if filename in filetypecounter[f]["filelist"] and filename not in filetypecounter[f]["loadedfiles"] and filestatus not in [0, 1]:
                        filetypecounter[f]["counter"] += 1
                        filetypecounter[f]["loadedfiles"].append(filename)
                if filetype == f and filestatus in [0, 1]:
                    level_servers = self.get_level_servers(age)
                    files[filename] = (nr_articles, age, filetype, False, True)
                    infolist[filename] = [None] * nr_articles
                    # self.pwdb.db_file_update_status(filename, 1)   # status do downloading
                    self.pwdb.exc("db_file_update_status", [filename, 1], {})   # status do downloading
                    for i, art0 in enumerate(file_articles):
                        if i == 0:
                            continue
                        art_nr, art_name, art_bytescount = art0
                        art_found = False
                        if self.resqlist:
                            for fn_r, age_r, ft_r, nr_art_r, art_nr_r, art_name_r, download_server_r, inf0_r, _ in self.resqlist:
                                if art_name == art_name_r:
                                    art_found = True
                                    self.resultqueue.put((fn_r, age_r, ft_r, nr_art_r, art_nr_r, art_name_r, download_server_r, inf0_r, False))
                                    break
                        if not art_found:
                            bytescount0 += art_bytescount
                            self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, level_servers))
                        article_count += 1
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
        # articles_processed = 0
        while True:
            # if articles_processed > 10000:
            #     break
            try:
                resultarticle = self.resultqueue.get_nowait()
                self.resultqueue.task_done()
                filename, age, filetype, nr_articles, art_nr, art_name, download_server, inf0, add_bytes = resultarticle
                if inf0 == "failed":
                    failed += 1
                    inf0 = empty_yenc_article
                    self.logger.error(whoami() + filename + "/" + art_name + ": failed!!")
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
            # articles_processed += 1
        if len(avgmiblist) > 50:
            avgmiblist = avgmiblist[:50]
        return newresult, avgmiblist, infolist, files, failed

    def clear_queues_and_pipes(self, onlyarticlequeue=False):
        self.logger.debug(whoami() + "starting clearing queues & pipes")

        # clear articlequeue
        while True:
            try:
                self.articlequeue.get_nowait()
                self.articlequeue.task_done()
            except (queue.Empty, EOFError, ValueError):
                break
            except Exception as e:
                self.logger.error(whoami() + str(e))
                return False
        self.articlequeue.join()
        if onlyarticlequeue:
            return True

        # clear resultqueue
        while True:
            try:
                self.resultqueue.get_nowait()
                self.resultqueue.task_done()
            except (queue.Empty, EOFError, ValueError):
                break
            except Exception as e:
                self.logger.error(whoami() + str(e))
                return False
        self.resultqueue.join()

        # clear pipes
        try:
            for key, item in self.pipes.items():
                if self.pipes[key][0].poll():
                    self.pipes[key][0].recv()
        except Exception as e:
            self.logger.error(whoami() + str(e))
            return False

        self.logger.debug(whoami() + "clearing queues & pipes done!")
        return True

    # do sanitycheck on nzb (excluding articles for par2vols)
    def do_sanity_check(self, allfileslist, files, infolist, bytescount0, filetypecounter):
        self.logger.info(whoami() + "performing sanity check")
        for t, _ in self.ct.threads:
            t.mode = "sanitycheck"
        sanity_injects = ["rar", "sfv", "nfo", "etc", "par2"]
        files, infolist, bytescount0, article_count = self.inject_articles(sanity_injects, allfileslist, files, infolist, bytescount0, filetypecounter)
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
    # def download(self, nzbname, allfileslist, filetypecounter, servers_shut_down):
    def run(self):
        # def download(self, nzbname, servers_shut_down):
        nzbname = self.nzbname

        res = self.pwdb.exc("db_nzb_get_allfile_list", [nzbname], {})
        allfileslist, filetypecounter, overall_size, overall_size_wparvol, already_downloaded_size, p2 = res

        try:
            nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
            fn = self.dirs["incomplete"] + nzbdir + "rq_" + nzbname + ".gzbx"
            with open(fn, "rb") as fp:
                resqlist = pickle.load(fp)
            self.resqlist = resqlist
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ": cannot load resqlist from file")
            self.resqlist = None

        # resqlist = self.pwdb.exc("db_nzb_get_resqlist", [nzbname], {})
        # self.resqlist = resqlist

        self.logger.info(whoami() + "downloading " + nzbname)
        self.pwdb.exc("db_msg_insert", [nzbname, "initializing download", "info"], {})

        # init variables
        self.logger.debug(whoami() + "download: init variables")
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
        if self.pwdb.exc("db_nzb_getstatus", [nzbname], {}) > 2:
            self.logger.info(nzbname + "- download complete!")
            self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                      overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), "dl_finished", self.main_dir
            sys.exit()
        self.pwdb.exc("db_nzb_update_status", [nzbname, 2], {})    # status "downloading"

        if filetypecounter["par2vol"]["max"] > 0:
            self.contains_par_files = True

        # which set of filetypes should I download
        self.logger.debug(whoami() + "download: define inject set")
        if filetypecounter["par2"]["max"] > 0 and filetypecounter["par2"]["max"] > filetypecounter["par2"]["counter"]:
            inject_set0 = ["par2"]
        elif self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            inject_set0 = ["etc", "par2vol", "rar", "sfv", "nfo"]
            loadpar2vols = True
        else:
            inject_set0 = ["etc", "sfv", "nfo", "rar"]
        self.logger.info(whoami() + "Overall_Size: " + str(overall_size) + ", incl. par2vols: " + str(overall_size_wparvol))

        # make dirs
        self.logger.debug(whoami() + "creating dirs")
        self.make_dirs(nzbname)
        self.sighandler.dirs = self.dirs

        self.pipes["renamer"][0].send(("start", self.download_dir, self.rename_dir))

        self.sighandler.mpp = self.mpp
        self.sighandler.nzbname = nzbname

        if self.ct.threads == []:
            # start download threads
            self.logger.debug(whoami() + "starting download threads")
            self.ct.start_threads()
            self.sighandler.servers = self.ct.servers

        bytescount0 = self.getbytescount(allfileslist)

        # sanity check
        inject_set_sanity = []
        if self.cfg["OPTIONS"]["SANITY_CHECK"].lower() == "yes" and not self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            sanity0 = self.do_sanity_check(allfileslist, files, infolist, bytescount0, filetypecounter)
            if sanity0 < 1:
                self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                overall_size = overall_size_wparvol
                self.logger.info(whoami() + "queuing par2vols")
                inject_set_sanity = ["par2vol"]

        # inject articles and GO!
        files, infolist, bytescount0, article_count = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0, filetypecounter)

        # inject par2vols because of sanity check
        if inject_set_sanity:
            files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set_sanity, allfileslist, files, infolist, bytescount0, filetypecounter)
            bytescount0 += bytescount00
            article_count += article_count0

        getnextnzb = False
        article_failed = 0

        # reset bytesdownloaded
        self.ct.reset_timestamps()

        # download loop until articles downloaded
        oldrarcounter = 0
        self.pwdb.exc("db_msg_insert", [nzbname, "downloading", "info"], {})

        while not self.stopped:

            return_reason = None

            if self.paused:
                time.sleep(0.2)
                continue

            self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                      overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), "N/A", self.main_dir

            # if dl is finished
            if getnextnzb:
                self.logger.info(nzbname + "- download success!")
                return_reason = "download success"
                self.pwdb.exc("db_nzb_update_status", [nzbname, 3], {})
                self.stop()
                continue

            # get renamer_result_queue (renamer.py)
            while True:
                try:
                    filename, full_filename, filetype, old_filename, old_filetype = self.renamer_result_queue.get_nowait()
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
                        filetypecounter[filetype]["loadedfiles"].append(filename)
                        # update allfileslist
                        for i, o_lists in enumerate(allfileslist):
                            o_orig_name, o_age, o_type, o_nr_articles = o_lists[0]
                            if o_orig_name == old_filename:
                                allfileslist[i][0] = (filename, o_age, o_type, o_nr_articles)
                    else:
                        self.logger.debug(whoami() + "moved " + filename + " to renamed dir")
                        filetypecounter[filetype]["counter"] += 1
                        filetypecounter[filetype]["loadedfiles"].append(filename)
                    if (filetype == "par2" or filetype == "par2vol") and not p2:
                        p2 = Par2File(full_filename)
                        self.logger.info(whoami() + "found first par2 file")
                    if inject_set0 == ["par2"] and (filetype == "par2" or filetypecounter["par2"]["max"] == 0):
                        self.logger.debug(whoami() + "injecting rars etc.")
                        inject_set0 = ["etc", "sfv", "nfo", "rar"]
                        files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0,
                                                                                             filetypecounter)
                        bytescount0 += bytescount00
                        article_count += article_count0
                except (queue.Empty, EOFError):
                    break
                # break

            # get mp_parverify_inqueue
            # self.pipes["renamer"][0].send(("pause", None, None))

            if not loadpar2vols:
                if self.pipes["verifier"][0].poll():
                    loadpar2vols = self.pipes["verifier"][0].recv()
                # while True:
                #    try:
                #        loadpar2vols = self.mp_parverify_inqueue.get_nowait()
                #    except (queue.Empty, EOFError):
                #        break
                if loadpar2vols:
                    # self.pwdb.db_nzb_update_loadpar2vols(nzbname, True)
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    overall_size = overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0,
                                                                                         filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0

            # check if unrarer is dead due to wrong rar on start
            # if self.mpp["unrarer"] and self.pwdb.db_nzb_get_unrarstatus(nzbname) == -2:
            if self.mpp["unrarer"] and self.pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) == -2:
                self.mpp["unrarer"].join()
                self.mpp["unrarer"] = None
                self.sighandler.mpp = self.mpp

            # if self.mpp["verifier"] and self.pwdb.db_nzb_get_verifystatus(nzbname) == 2:
            if self.mpp["verifier"] and self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) == 2:
                self.mpp["verifier"].join()
                self.mpp["verifier"] = None
                self.sighandler.mpp = self.mpp

            # if unrarer is running and verifystatus is negative, stop!
            if self.mpp["unrarer"] and self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) == -2:
                self.logger.debug(whoami() + "unrarer running but stopping/postponing now due to broken rar file!")
                os.kill(self.mpp["unrarer"].pid, signal.SIGTERM)
                self.mpp["unrarer"].join()
                self.mpp["unrarer"] = None
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
                        return_reason = "download failed"
                        # self.pwdb.db_msg_insert(nzbname, "download failed due to pw test not possible", "error")
                        self.pwdb.exc("db_msg_insert", [nzbname, "download failed due to pw test not possible", "error"], {})
                        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                                  overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), return_reason, self.main_dir
                        self.stop()
                        continue
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
                        verifystatus = self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
                        if verifystatus != -2:
                            self.logger.debug(whoami() + "starting unrarer")
                            self.mpp_unrarer = mp.Process(target=partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                                      nzbname, self.logger, None, self.event_unrareridle, self.cfg, ))
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
                        self.mpp_verifier = mp.Process(target=par_verifier, args=(self.pipes["verifier"][1], self.rename_dir, self.verifiedrar_dir,
                                                                                  self.main_dir, self.logger, nzbname, pvmode, self.event_verifieridle, self.cfg, ))
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
            # stop if par2file cannot be downloaded
            par2failed = False
            if failed != 0:
                for fname, item in files.items():
                    f_nr_articles, f_age, f_filetype, _, failed0 = item
                    if (failed0 and f_filetype == "par2") or (infolist[fname] == None and f_filetype == "par2"):
                        par2failed = True
                        break
                self.logger.warning(whoami() + str(failed) + " articles failed, article_count: " + str(article_count) + ", health: " + str(article_health)
                                    + ", par2failed: " + str(par2failed))
                # if too many missing articles: exit download
                if (article_health < self.crit_art_health_wo_par and filetypecounter["par2vol"]["max"] == 0) \
                   or par2failed \
                   or (filetypecounter["parvols"]["max"] > 0 and article_health <= self.crit_art_health_w_par):
                    self.logger.info(whoami() + "articles missing and cannot repair, exiting download")
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                    self.pwdb.exc("db_msg_insert", [nzbname, "critical health threashold exceeded", "error"], {})
                    if par2failed:
                        self.pwdb.exc("db_msg_insert", [nzbname, "par2 file broken/not available on servers", "error"], {})
                    return_reason = "download failed"
                    self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                              overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), return_reason, self.main_dir
                    self.stop()
                    continue
                if not loadpar2vols and filetypecounter["parvols"]["max"] > 0 and article_health > 0.95:
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0,
                                                                                         filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0
                    overall_size = overall_size_wparvol
                    loadpar2vols = True

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
                self.pwdb.exc("db_msg_insert", [nzbname, "inconsistency in download queue", "error"], {})
                self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                self.logger.warning(whoami() + ": records say dl is done, but still some articles in queue, exiting ...")
                return_reason = "download failed"
                self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                          overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), return_reason, self.main_dir
                self.stop()
                continue

        if not return_reason:
            return_reason = "download terminated!"
            msgtype = "warning"
        elif return_reason.endswith("failed"):
            msgtype = "error"
        elif return_reason.endswith("success"):
            msgtype = "success"
        else:
            msgtype = "info"
        self.pwdb.exc("db_msg_insert", [nzbname, return_reason, msgtype], {})
        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                  overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), return_reason, self.main_dir

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


def get_next_nzb(pwdb, dirs, ct, guiconnector, logger):
    # waiting for nzb_parser to insert all nzbs in nzbdir into db ---> this is a problem, because startup takes
    # long with many nzbs!!
    tt0 = time.time()
    while True:
        if guiconnector.closeall:
            return False, 0
        if guiconnector.has_first_nzb_changed():
            if not not guiconnector.has_nzb_been_deleted():
                return False, -10     # return_reason = "nzbs_reordered"
            else:
                return False, -20     # return_reason = "nzbs_deleted"
        try:
            nextnzb = pwdb.exc("db_nzb_getnextnzb_for_download", [], {})
            if nextnzb:
                break
        except Exception as e:
            pass
        if ct.threads and time.time() - tt0 > 30:
            logger.debug(whoami() + "idle time > 30 sec, closing threads & connections")
            ct.stop_threads()
        time.sleep(0.5)

    logger.debug(whoami() + "looking for new NZBs ...")
    try:
        nzbname = make_allfilelist_wait(pwdb, dirs, guiconnector, logger, -1)
    except Exception as e:
        logger.warning(whoami() + str(e))
    if nzbname == -1:
        return False, 0
    # poll for 30 sec if no nzb immediately found
    if not nzbname:
        logger.debug(whoami() + "polling for 30 sec. for new NZB before closing connections if alive ...")
        nzbname = make_allfilelist_wait(pwdb, dirs, guiconnector, logger, 30 * 1000)
        if nzbname == -1:
            return False, 0
        if not nzbname:
            if ct.threads:
                # if no success: close all connections and poll blocking
                logger.debug(whoami() + "idle time > 30 sec, closing all threads + server connections")
                ct.stop_threads()
            logger.debug(whoami() + "polling for new nzbs now in blocking mode!")
            try:
                nzbname = make_allfilelist_wait(pwdb, dirs, guiconnector, logger, None)
                if nzbname == -1:
                    return False, 0
            except Exception as e:
                logger.warning(whoami() + str(e))
    pwdb.exc("store_sorted_nzbs", [], {})
    return True, nzbname


def make_allfilelist_wait(pwdb, dirs, guiconnector, logger, timeout0):
    # immediatley get allfileslist
    try:
        nzbname = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
        if nzbname:
            logger.debug(whoami() + "no timeout, got nzb " + nzbname + " immediately!")
            return nzbname
        elif timeout0 and timeout0 <= -1:
            return None
    except Exception as e:
        logger.warning(whoami() + str(e))
        return None
    # setup inotify
    logger.debug(whoami() + "waiting for new nzb with timeout=" + str(timeout0))
    t0 = time.time()
    if not timeout0:
        delay0 = 5
    else:
        delay0 = 1
    while True:
        if guiconnector.closeall:
            return -1
        try:
            nzbname = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
        if nzbname:
            logger.debug(whoami() + "new nzb found in db, queuing ...")
            return nzbname
        if timeout0:
            if time.time() - t0 > timeout0 / 1000:
                break
        time.sleep(delay0)
    return None


def write_resultqueue_to_file(resultqueue, dirs, pwdb, nzbname, logger):
    if not nzbname:
        return 0
    logger.debug(whoami() + "reading " + nzbname + "resultqueue and writing to file")
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
            resqlist = None
            break
    nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
    fn = dirs["incomplete"] + nzbdir + "rq_" + nzbname + ".gzbx"
    if resqlist:
        try:
            with open(fn, "wb") as fp:
                pickle.dump(resqlist, fp)
        except Exception as e:
            logger.warning(whoami() + str(e) + ": cannot write " + fn)
    else:
        try:
            os.remove(fn)
        except Exception as e:
            logger.warning(whoami() + str(e) + ": cannot remove resqueue.gzbx")
    logger.debug(whoami() + "reading resultqueue and writing to file, done!")
    return bytes_in_resultqueue


def write_resultqueue_to_db(resultqueue, maindir, pwdb, nzbname, logger):
    logger.debug(whoami() + "reading " + nzbname + "resultqueue and writing to db")
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
    if resqlist:
        pwdb.exc("db_nzb_store_resqlist", [nzbname, resqlist], {})
    logger.debug(whoami() + "reading resultqueue and writing to db, done!")
    return bytes_in_resultqueue


def clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, logger, stopall=False):
    # join all queues
    logger.debug(whoami() + "clearing articlequeue")
    if dl:
        dl.clear_queues_and_pipes(onlyarticlequeue=True)
    # 2. wait for all remaining articles to be downloaded
    '''logger.debug(whoami() + "waiting for all remaining articles to be downloaded")
    dl_not_done_yet = True
    while dl_not_done_yet:
        dl_not_done_yet = False
        for t, _ in dl.ct.threads:
            if not t.is_download_done():
                dl_not_done_yet = True
                break
        if dl_not_done_yet:
            time.sleep(0.2)'''
    # 3. stop article_decoder
    if stopall and dl:
        mpid = None
        try:
            if dl.mpp["decoder"]:
                mpid = dl.mpp["decoder"].pid
            if mpid:
                logger.warning("terminating decoder")
                try:
                    os.kill(dl.mpp["decoder"].pid, signal.SIGTERM)
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
    if dl:
        logger.debug(whoami() + "writing resultqueue")
        bytes_in_resultqueue = write_resultqueue_to_file(resultqueue, dirs, pwdb, nzbname, logger)
        pwdb.exc("db_nzb_set_bytes_in_resultqueue", [nzbname, bytes_in_resultqueue], {})
    # 6. stop unrarer
    if dl:
        logger.debug(whoami() + "stopping unrarer")
        mpid = None
        try:
            if dl.mpp["unrarer"]:
                mpid = dl.mpp["unrarer"].pid
            if mpid:
                # if self.mpp["unrarer"].pid:
                logger.warning("terminating unrarer")
                try:
                    os.kill(mpid, signal.SIGTERM)
                    dl.mpp["unrarer"].join()
                    dl.mpp["unrarer"] = None
                    dl.sighandler.mpp = dl.mpp
                except Exception as e:
                    logger.debug(whoami() + str(e))
        except Exception as e:
            logger.debug(whoami() + ": " + str(e))
    # 7. stop rar_verifier
    if dl:
        logger.debug(whoami() + "stopping par_verifier")
        mpid = None
        try:
            if dl.mpp["verifier"]:
                mpid = dl.mpp["verifier"].pid
            if mpid:
                logger.warning(whoami() + "terminating rar_verifier")
                try:
                    os.kill(dl.mpp["verifier"].pid, signal.SIGTERM)
                    dl.mpp["verifier"].join()
                    dl.mpp["verifier"] = None
                    dl.sighandler.mpp = dl.mpp
                except Exception as e:
                    logger.debug(whoami() + str(e))
        except Exception as e:
            logger.debug(whoami() + ": " + str(e))
    # 8. stop mpp_renamer
    if pipes:
        try:
            logger.debug(whoami() + "stopping renamer")
            pipes["renamer"][0].send(("pause", None, None))
        except Exception as e:
            logger.warning(whoami() + str(e))
    # 9. stop post-proc
    logger.debug(whoami() + "stopping post_processor")
    mpid = None
    try:
        if mpp["post"]:
            mpid = mpp["post"].pid
        if mpid:
            logger.warning(whoami() + "terminating postprocesspr")
            try:
                os.kill(mpid, signal.SIGTERM)
                mpp["post"].join()
                mpp["post"] = None
                if dl:
                    dl.sighandler.mpp["post"] = None
            except Exception as e:
                logger.debug(whoami() + str(e))
    except Exception as e:
        logger.debug(whoami() + ": " + str(e))
    # 10. stop nzbparser
    if stopall and dl:
        logger.debug(whoami() + "stopping nzb_parser")
        try:
            mpid = None
            if dl.mpp["nzbparser"]:
                mpid = dl.mpp["nzbparser"].pid
            if mpid:
                logger.debug(whoami() + "terminating nzb_parser")
                try:
                    os.kill(dl.mpp["nzbparser"].pid, signal.SIGTERM)
                    dl.mpp["nzbparser"].join()
                except Exception as e:
                    logger.debug(whoami() + str(e))
        except Exception as e:
            logger.debug(whoami() + str(e))
    return


def connection_thread_health(threads):
        nothreads = len([t for t, _ in threads])
        nodownthreads = len([t for t, _ in threads if t.connectionstate == -1])
        if nothreads == 0:
            return 0
        return 1 - nodownthreads / (nothreads)


# main loop for ginzibix downloader
def ginzi_main(cfg, dirs, subdirs, logger):

    logger.debug(whoami() + "starting ...")

    nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2 = (None, ) * 6

    pwdb = PWDBSender()

    logger.debug(whoami() + "init monitor status to 'not started'")
    pwdb.exc("db_status_init", [], {})

    mp_events = {}
    mp_events["unrarer"] = mp.Event()
    mp_events["verifier"] = mp.Event()
    mp_events["post"] = mp.Event()

    mp_work_queue = mp.Queue()
    articlequeue = queue.LifoQueue()
    resultqueue = queue.Queue()
    renamer_result_queue = mp.Queue()

    renamer_parent_pipe, renamer_child_pipe = mp.Pipe()
    unrarer_parent_pipe, unrarer_child_pipe = mp.Pipe()
    verifier_parent_pipe, verifier_child_pipe = mp.Pipe()
    pipes = {"renamer": [renamer_parent_pipe, renamer_child_pipe],
             "unrarer": [unrarer_parent_pipe, unrarer_child_pipe],
             "verifier": [verifier_parent_pipe, verifier_child_pipe]}

    ct = ConnectionThreads(cfg, articlequeue, resultqueue, cfg, logger)

    # init sighandler
    logger.debug(whoami() + "initializing sighandler")
    mpp = {"nzbparser": None, "decoder": None, "unrarer": None, "renamer": None, "verifier": None, "post": None}
    sh = SigHandler_Main(mpp, ct, mp_work_queue, resultqueue, articlequeue, pwdb, logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    # start nzb parser mpp
    logger.debug(whoami() + "starting nzbparser process ...")
    mpp_nzbparser = mp.Process(target=ParseNZB, args=(cfg, dirs, logger, ))
    mpp_nzbparser.start()
    mpp["nzbparser"] = mpp_nzbparser

    # start decoder mpp
    logger.debug(whoami() + "starting decoder process ...")
    mpp_decoder = mp.Process(target=decode_articles, args=(mp_work_queue, cfg, logger, ))
    mpp_decoder.start()
    mpp["decoder"] = mpp_decoder

    # start renamer
    logger.debug(whoami() + "starting renamer process ...")
    mpp_renamer = mp.Process(target=renamer, args=(renamer_child_pipe, renamer_result_queue, logger, ))
    mpp_renamer.start()
    mpp["renamer"] = mpp_renamer

    sh.mpp = mpp

    try:
        lock = threading.Lock()
        guiconnector = GUI_Connector(lock, dirs, logger, cfg)
        guiconnector.start()
        logger.debug(whoami() + "guiconnector process started!")
    except Exception as e:
        logger.warning(whoami() + str(e))

    dl = None
    nzbname = None
    paused = False
    guiconnector.set_health(0, 0)
    article_health = 0
    connection_health = 0
    while True:

        time.sleep(0.1)

        try:
            pwdb.exc("store_sorted_nzbs", [], {})

            # closeall command
            if guiconnector.closeall:
                logger.info(whoami() + "got closeall")
                try:
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, logger, stopall=True)
                except Exception as e:
                    logger.debug(whoami() + str(e))
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                if dl:
                    dl.stop()
                    dl.join()
                    dl = None
                if mpp["post"]:
                    mpp["post"].join()
                sh.shutdown()

            # set connection health
            connection_health = connection_thread_health(ct.threads)
            guiconnector.set_health(article_health, connection_health)

            # if not downloading
            if not dl:
                nzbname = make_allfilelist_wait(pwdb, dirs, guiconnector, logger, -1)
                if nzbname:
                    sh.nzbname = nzbname
                    ct.reset_timestamps_bdl()
                    logger.info(whoami() + "got next NZB: " + str(nzbname))
                    dl = Downloader(cfg, dirs, ct, mp_work_queue, sh, mpp, guiconnector, pipes, renamer_result_queue, mp_events, nzbname, logger)
                    dl.start()
                else:
                    sh.nzbname = None
                    sh.dirs = None
                    time.sleep(0.5)
                    continue
            else:
                stat0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
                if stat0 == 2:
                    statusmsg = "downloading"
                elif stat0 == 3:
                    statusmsg = "postprocessing"
                elif stat0 == 4:
                    statusmsg = "success"
                elif stat0 == -4:
                    statusmsg = "failed"
                # send data to gui
                if dl.results:
                    try:
                        nzbname, downloaddata, return_reason, maindir = dl.results
                        bytescount0, availmem0, avgmiblist, filetypecounter, _, article_health, overall_size, already_downloaded_size, p2, overall_size_wparvol,\
                            allfileslist = downloaddata
                        downloaddata_gc = bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health, overall_size, already_downloaded_size
                        # no ct.servers object if connection idle
                        if not ct.servers:
                            serverconfig = None
                        else:
                            serverconfig = ct.servers.server_config
                        guiconnector.set_data(downloaddata_gc, ct.threads, serverconfig, statusmsg, dl.serverhealth())
                    except Exception as e:
                        logger.debug(whoami() + str(e) + ": cannot interpret gui-data from downloader")
                # status downloading
                if stat0 in [2, 3]:
                    # command pause
                    if not guiconnector.dl_running and not paused:
                        paused = True
                        logger.info(whoami() + "download paused for NZB " + nzbname)
                        ct.pause_threads()
                        if dl:
                            dl.pause()
                            # dl.ct.reset_timestamps_bdl()
                        postproc_pause()
                    # command resume
                    elif guiconnector.dl_running and paused:
                        logger.info(whoami() + "download resumed for NZB " + nzbname)
                        paused = False
                        ct.resume_threads()
                        if dl:
                            dl.resume()
                        postproc_resume()
                # if download ok -> postprocess
                if stat0 == 3 and not mpp["post"]:
                    guiconnector.set_health(0, 0)
                    logger.info(whoami() + "download success, postprocessing NZB " + nzbname)
                    mpp_post = mp.Process(target=postprocess_nzb, args=(nzbname, articlequeue, resultqueue, mp_work_queue, pipes, mpp, mp_events, cfg,
                                                                        dl.verifiedrar_dir, dl.unpack_dir, dl.nzbdir, dl.rename_dir, dl.main_dir,
                                                                        dl.download_dir, dl.dirs, dl.pw_file, mp_events["post"], logger, ))
                    mpp_post.start()
                    mpp["post"] = mpp_post
                    sh.mpp["post"] = mpp_post
                # if download failed
                elif stat0 == -2:
                    logger.info(whoami() + "download failed for NZB " + nzbname)
                    pwdb.exc("db_msg_insert", [nzbname, "download failed!", "error"], {})
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, logger)
                    pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                    # set 'flags' for getting next nzb
                    dl.stop()
                    dl.join()
                    ct.pause_threads()
                    del dl
                    dl = None
                    nzbname = None
                # if postproc ok
                elif stat0 == 4:
                    logger.info(whoami() + "postprocessor success for NZB " + nzbname)
                    mpp["post"].join()
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, logger)
                    pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                    guiconnector.set_data(downloaddata_gc, ct.threads, ct.servers.server_config, "success", dl.serverhealth())
                    pwdb.exc("db_msg_insert", [nzbname, "downloaded and postprocessed successfully!", "success"], {})
                    mpp["post"] = None
                    sh.mpp["post"] = None
                    # set 'flags' for getting next nzb
                    dl.stop()
                    dl.join()
                    ct.pause_threads()
                    del dl
                    dl = None
                    nzbname = None
                # if postproc failed
                elif stat0 == -4:
                    logger.error(whoami() + "postprocessor failed for NZB " + nzbname)
                    mpp["post"].join()
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, logger)
                    pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                    guiconnector.set_data(downloaddata_gc, ct.threads, ct.servers.server_config, "failed", dl.serverhealth())
                    pwdb.exc("db_msg_insert", [nzbname, "download and/or postprocessing failed!", "error"], {})
                    mpp["post"] = None
                    sh.mpp = mpp
                    # set 'flags' for getting next nzb
                    dl.stop()
                    dl.join()
                    ct.pause_threads()
                    del dl
                    dl = None
                    nzbname = None
        except Exception as e:
            print(str(e))

    '''nzbname, downloaddata, return_reason, maindir = dl.results
            bytescount0, availmem0, avgmiblist, filetypecounter, _, article_health, overall_size, already_downloaded_size, p2, overall_size_wparvol, \
                allfileslist = downloaddata
ack             downloaddata_gc = bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health, overall_size, already_downloaded_size
            stat0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
            logger.debug(whoami() + "downloader exited with status: " + str(stat0))
        if return_reason == "closeall":
            logger.debug(whoami() + "closing down")
            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger, stopall=True)
            pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
            sh.shutdown()
        if stat0 == 2:
            if return_reason == "connection_restart":
                ct.stop_threads()
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger)
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                servers_shut_down = True
                time.sleep(1)
                continue
            elif return_reason == "nzbs_reordered":
                logger.debug(whoami() + "NZBs have been reordered")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger)
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                continue
            elif return_reason == "nzbs_deleted":
                logger.debug(whoami() + "NZBs have been deleted")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger)
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                deleted_nzb_name0 = guiconnector.has_nzb_been_deleted(delete=True)
                remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger)
                continue
            elif return_reason == "dl_stopped":
                logger.debug(whoami() + "download paused")
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger)
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                # idle until start or nzbs_reordered signal comes from gtkgui
                idlestart = time.time()
                servers_shut_down = False
                dl.ct.reset_timestamps_bdl()
                guiconnector.set_health(0, 0)
                while True:
                    dobreak = False
                    with guiconnector.lock:
                        if guiconnector.closeall:
                            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger, stopall=True)
                            pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                            sh.shutdown()
                        if guiconnector.dl_running:
                            dobreak = True
                        if guiconnector.has_first_nzb_changed():
                            deleted_nzb_name0 = guiconnector.has_nzb_been_deleted(delete=True)
                            if deleted_nzb_name0:
                                remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger)
                            pwdb.exc("store_sorted_nzbs", [], {})
                    if dobreak:
                        break
                    time.sleep(1)
                    # after 2min idle -> stop threads
                    if not servers_shut_down and time.time() - idlestart > 2 * 60:
                        ct.stop_threads()
                        servers_shut_down = True
                    time.sleep(1)
                continue

        # if download success, postprocess
        elif stat0 == 3:
            guiconnector.set_health(0, 0)
            logger.info(whoami() + "download success, postprocessing NZB " + nzbname)
            # dl.postprocess_nzb(nzbname, downloaddata)
            try:
                guiconnector.set_data(downloaddata_gc, ct.threads, ct.servers.server_config, "postprocessing", dl.serverhealth())
                mpp_post = mp.Process(target=postprocess_nzb, args=(nzbname, articlequeue, resultqueue, mp_work_queue, pipes, mpp, mp_events, cfg,
                                                                    dl.verifiedrar_dir, dl.unpack_dir, dl.nzbdir, dl.rename_dir, dl.main_dir, dl.download_dir,
                                                                    dl.dirs, dl.pw_file, mp_events["post"], logger, ))
                mpp_post.start()
                mpp["post"] = mpp_post
            except Exception as e:
                logger.error(whoami() + str(e))
            logger.debug(whoami() + "waiting for postprocessor to finish")
            mpp["post"].join()
            logger.debug(whoami() + "postprocessor finished!")
            stat0_0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger)
            pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
            if stat0_0 == 4:
                guiconnector.set_data(downloaddata_gc, ct.threads, ct.servers.server_config, "success", dl.serverhealth())
                pwdb.exc("db_msg_insert", [nzbname, "downloaded and postprocessed successfully!", "success"], {})
            else:
                guiconnector.set_data(downloaddata_gc, ct.threads, ct.servers.server_config, "failed", dl.serverhealth())
                pwdb.exc("db_msg_insert", [nzbname, "download and/or postprocessing failed!", "error"], {})
            logger.info(whoami() + nzbname + " finished with status " + str(stat0_0))
        elif stat0 == -2:
            guiconnector.set_health(0, 0)
            pwdb.exc("db_msg_insert", [nzbname, "download failed!", "error"], {})
            logger.info(whoami() + "download failed for NZB " + nzbname)
            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, logger)
            pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})'''
