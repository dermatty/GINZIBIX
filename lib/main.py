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
from .guiconnector import GUI_Connector, remove_nzb_files_and_db
from .postprocessor import postprocess_nzb, postproc_pause, postproc_resume
from .mplogging import setup_logger, whoami
from setproctitle import setproctitle


empty_yenc_article = [b"=ybegin line=128 size=14 name=ginzi.txt",
                      b'\x9E\x92\x93\x9D\x4A\x93\x9D\x4A\x8F\x97\x9A\x9E\xA3\x34\x0D\x0A',
                      b"=yend size=14 crc32=8111111c"]


_ftypes = ["etc", "rar", "sfv", "par2", "par2vol"]


class SigHandler_Main:

    def __init__(self, event_stopped, logger):
        self.logger = logger
        self.event_stopped = event_stopped

    def sighandler(self, a, b):
        self.event_stopped.set()
        self.logger.debug(whoami() + "set event_stopped = True")


# Handles download of a NZB file
class Downloader(Thread):
    def __init__(self, cfg, dirs, ct, mp_work_queue, mpp, guiconnector, pipes, renamer_result_queue, mp_events,
                 nzbname, mp_loggerqueue, logger):
        Thread.__init__(self)
        self.daemon = True
        self.lock = threading.Lock()
        self.mp_loggerqueue = mp_loggerqueue
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
        self.mpp = mpp
        self.guiconnector = guiconnector
        self.resqlist = []
        self.article_health = 1
        self.connection_health = 1
        self.contains_par_files = False
        self.results = None
        self.read_cfg()
        self.event_stopped = threading.Event()
        self.event_paused = threading.Event()
        self.stopped_counter = 0

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
        self.event_stopped.set()
        with self.lock:
            self.stopped_counter = 0

    def pause(self):
        self.event_paused.set()

    def resume(self):
        self.event_paused.clear()

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
        global empty_yenc_article
        # read resultqueue + distribute to files
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
                # if inf0 == 0 -> this is a remains of sanity-check (should not happen but sometimes does ...)
                if inf0 == 0:
                    continue
                if inf0 == "failed":
                    failed += 1
                    inf0 = empty_yenc_article
                    self.logger.error(whoami() + filename + "/" + art_name + ": failed!!")
                bytesdownloaded = 0
                if add_bytes:
                    try:
                        bytesdownloaded = sum(len(i) for i in inf0)
                    except Exception as e:
                        bytesdownloaded = 0
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
                    if b"name=ginzi.txt" in infolist[filename][0][0]:
                        failed0 = True
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
                _, _, _, _, _, artname, _, status, _ = resultarticle
                if status != "failed":
                    nr_ok_articles += 1
                else:
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "cannot download " + artname, "info"], {})
                    self.logger.debug(whoami() + "cannot download " + artname)
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

        # start decoder mpp
        self.logger.debug(whoami() + "starting decoder process ...")
        self.mpp_decoder = mp.Process(target=decode_articles, args=(self.mp_work_queue, self.mp_loggerqueue, ))
        self.mpp_decoder.start()
        self.mpp["decoder"] = self.mpp_decoder

        # start dl threads
        self.logger.info(whoami() + "starting/resuming download threads")
        self.ct.resume_threads()

        # def download(self, nzbname, servers_shut_down):
        nzbname = self.nzbname
        return_reason = None

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
                                      overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), "download complete", self.main_dir
            sys.exit()

        # make dirs
        self.logger.debug(whoami() + "creating dirs")
        self.make_dirs(nzbname)

        # trigger set_data for gui
        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                  overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), "N/A", self.main_dir
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

        self.pipes["renamer"][0].send(("start", self.download_dir, self.rename_dir))

        bytescount0 = self.getbytescount(allfileslist)

        # sanity check
        inject_set_sanity = []
        sanity0 = -1
        if self.cfg["OPTIONS"]["SANITY_CHECK"].lower() == "yes" and not self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            self.pwdb.exc("db_msg_insert", [nzbname, "checking for sanity", "info"], {})
            sanity0 = self.do_sanity_check(allfileslist, files, infolist, bytescount0, filetypecounter)
            if sanity0 < 1:
                if (filetypecounter["par2vol"]["max"] > 0 and sanity0 < self.crit_art_health_w_par) or\
                   (filetypecounter["par2vol"]["max"] == 0 and sanity0 < self.crit_art_health_wo_par):
                    self.pwdb.exc("db_msg_insert", [nzbname, "Sanity less than criticical health level, exiting", "error"], {})
                    self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                              overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), "download failed", self.main_dir
                    sys.exit()
                    
                self.pwdb.exc("db_msg_insert", [nzbname, "Sanity less than 100%, preloading par2vols!", "warning"], {})
                self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                overall_size = overall_size_wparvol
                self.logger.info(whoami() + "queuing par2vols")
                inject_set_sanity = ["par2vol"]
                self.ct.pause_threads()
                self.clear_queues_and_pipes()
                time.sleep(0.5)
                self.ct.resume_threads()
            else:
                self.pwdb.exc("db_msg_insert", [nzbname, "Sanity is 100%, all OK!", "info"], {})

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

        stopped_max_counter = 5

        while self.stopped_counter < stopped_max_counter:

            # if terminated: ensure that all tasks are processed
            if self.event_stopped.wait(0.25):
                self.stopped_counter += 1
                self.resume()
                self.logger.info(whoami() + "termination countdown: " + str(self.stopped_counter) + " of " + str(stopped_max_counter))
                time.sleep(0.25)

            return_reason = None

            if self.event_paused.isSet():
                continue

            self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                      overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), "N/A", self.main_dir

            # if dl is finished
            if getnextnzb:
                self.logger.info(nzbname + "- download success!")
                return_reason = "download success"
                self.pwdb.exc("db_nzb_update_status", [nzbname, 3], {})
                self.stop()
                self.stopped_counter = stopped_max_counter       # stop immediately
                continue

            # get renamer_result_queue (renamer.py)
            while True:
                try:
                    filename, full_filename, filetype, old_filename, old_filetype = self.renamer_result_queue.get_nowait()
                    self.pwdb.exc("db_msg_insert", [nzbname, "downloaded & renamed " + filename, "info"], {})
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

            # if verifier tells us so, we load par2vol files
            if not self.event_stopped.isSet() and not loadpar2vols:
                if self.pipes["verifier"][0].poll():
                    loadpar2vols = self.pipes["verifier"][0].recv()
                if loadpar2vols:
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar(s) corrupt, loading par2vol files", "info"], {})
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    overall_size = overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0,
                                                                                         filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0

            # check if unrarer is dead due to wrong rar on start
            if self.mpp["unrarer"] and self.pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) == -2:
                self.mpp["unrarer"].join()
                self.mpp["unrarer"] = None
                self.logger.debug(whoami() + "unrarer joined")

            if self.mpp["verifier"]:
                verifystatus = self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
                if verifystatus == 2:
                    self.mpp["verifier"].join()
                    self.mpp["verifier"] = None
                    self.logger.debug(whoami() + "verifier joined")
                # if unrarer is running and verifystatus is negative, stop!
                elif verifystatus == -2:
                    self.logger.debug(whoami() + "unrarer running but stopping/postponing now due to broken rar file!")
                    try:
                        os.kill(self.mpp["unrarer"].pid, signal.SIGTERM)
                        self.mpp["unrarer"].join()
                    except Exception:
                        pass
                    self.mpp["unrarer"] = None
                    self.pwdb.exc("db_nzb_update_unrar_status", [nzbname, 0], {})
                    self.pwdb.exc("db_msg_insert", [nzbname, "par repair needed, postponing unrar", "info"], {})

            if not self.event_stopped.isSet() and not self.mpp["unrarer"] and filetypecounter["rar"]["counter"] > oldrarcounter\
               and not self.pwdb.exc("db_nzb_get_ispw_checked", [nzbname], {}):
                # testing if pw protected
                rf = [rf0 for _, _, rf0 in os.walk(self.verifiedrar_dir) if rf0]
                # if no rar files in verified_rardir: skip as we cannot test for password
                if rf:
                    oldrarcounter = filetypecounter["rar"]["counter"]
                    self.logger.debug(whoami() + ": first/new verified rar file appeared, testing if pw protected")
                    is_pwp = is_rar_password_protected(self.verifiedrar_dir, self.logger)
                    self.pwdb.exc("db_nzb_set_ispw_checked", [nzbname, True], {})
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
                        self.stopped_counter = stopped_max_counter
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
                                                                                      nzbname, self.mp_loggerqueue, None, self.event_unrareridle, self.cfg, ))
                            self.mpp_unrarer.start()
                            self.mpp["unrarer"] = self.mpp_unrarer
                    elif is_pwp == -3:
                        self.logger.info(whoami() + ": cannot check for pw protection as first rar not present yet")
                else:
                    self.logger.debug(whoami() + "no rars in verified_rardir yet, cannot test for pw / start unrarer yet!")

            # if par2 available start par2verifier, else just copy rars unchecked!
            if not self.event_stopped.isSet() and not self.mpp["verifier"]:
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
                                                                                  self.main_dir, self.mp_loggerqueue, nzbname, pvmode, self.event_verifieridle,
                                                                                  self.cfg, ))
                        self.mpp_verifier.start()
                        self.mpp["verifier"] = self.mpp_verifier
                        self.logger.info(whoami() + "verifier started!")

            # read resultqueue + decode via mp
            newresult, avgmiblist, infolist, files, failed = self.process_resultqueue(avgmiblist, infolist, files)
            article_failed += failed
            if article_count != 0:
                article_health = 1 - article_failed / article_count
                if article_health > sanity0 and sanity0 != -1:
                    article_health = sanity0
            else:
                if sanity0 != -1:
                    article_health = sanity0
                else:
                    article_health = 0
            self.article_health = article_health
            # stop if par2file cannot be downloaded
            par2failed = False
            if failed != 0:
                for fname, item in files.items():
                    f_nr_articles, f_age, f_filetype, _, failed0 = item
                    if (failed0 and f_filetype == "par2"):
                        par2failed = True
                        break
                self.logger.warning(whoami() + str(failed) + " articles failed, article_count: " + str(article_count) + ", health: " + str(article_health)
                                    + ", par2failed: " + str(par2failed))
                # if too many missing articles: exit download
                if (article_health < self.crit_art_health_wo_par and filetypecounter["par2vol"]["max"] == 0) \
                   or par2failed \
                   or (filetypecounter["par2vol"]["max"] > 0 and article_health <= self.crit_art_health_w_par):
                    self.logger.info(whoami() + "articles missing and cannot repair, exiting download")
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                    if par2failed:
                        self.pwdb.exc("db_msg_insert", [nzbname, "par2 file broken/not available on servers", "error"], {})
                    else:
                        self.pwdb.exc("db_msg_insert", [nzbname, "critical health threashold exceeded", "error"], {})
                    return_reason = "download failed"
                    self.results = nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                                              overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist)), return_reason, self.main_dir
                    self.stop()
                    self.stopped_counter = stopped_max_counter
                    continue
                if not loadpar2vols and filetypecounter["par2vol"]["max"] > 0 and article_health > 0.95:
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0,
                                                                                         filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0
                    overall_size = overall_size_wparvol
                    loadpar2vols = True
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar(s) corrupt, loading par2vol files", "info"], {})

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
                self.stopped_counter = stopped_max_counter
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


def clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False, onlyarticlequeue=True):
    # 1. join & clear all queues
    if dl:
        dl.clear_queues_and_pipes(onlyarticlequeue)
        logger.info(whoami() + "articlequeue cleared!")
    # 2. stop article_decoder
    try:
        if mpp["decoder"]:
            mpid = mpp["decoder"].pid
            logger.debug(whoami() + "terminating decoder")
            os.kill(mpp["decoder"].pid, signal.SIGTERM)
            mpp["decoder"].join()
            mpp["decoder"] = None
            logger.info(whoami() + "decoder terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 4. clear mp_work_queue
    logger.debug(whoami() + "clearing mp_work_queue")
    while True:
        try:
            mp_work_queue.get_nowait()
        except (queue.Empty, EOFError):
            break
    logger.info(whoami() + "mp_work_queue cleared!")
    # 5. save resultqueue
    if dl:
        try:
            logger.debug(whoami() + "writing resultqueue")
            bytes_in_resultqueue = write_resultqueue_to_file(resultqueue, dirs, pwdb, nzbname, logger)
            pwdb.exc("db_nzb_set_bytes_in_resultqueue", [nzbname, bytes_in_resultqueue], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
    # 6. stop unrarer
    try:
        if mpp["unrarer"]:
            mpid = mpp["unrarer"].pid
            logger.debug("terminating unrarer")
            os.kill(mpid, signal.SIGTERM)
            mpp["unrarer"].join()
            mpp["unrarer"] = None
            logger.info(whoami() + "unrarer terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 7. stop rar_verifier
    try:
        if mpp["verifier"]:
            mpid = mpp["verifier"].pid
            logger.debug(whoami() + "terminating par_verifier")
            os.kill(mpp["verifier"].pid, signal.SIGTERM)
            mpp["verifier"].join()
            mpp["verifier"] = None
            logger.info(whoami() + "verifier terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 8. stop renamer only if stopall otherwise just pause
    if stopall:
        try:
            if mpp["renamer"]:
                mpid = mpp["renamer"].pid
                logger.debug(whoami() + "stopall: terminating renamer")
                os.kill(mpp["renamer"].pid, signal.SIGTERM)
                mpp["renamer"].join()
                mpp["renamer"] = None
                logger.info(whoami() + "renamer terminated!")
        except Exception as e:
            logger.debug(whoami() + str(e))
    # just pause
    elif pipes:
        try:
            logger.debug(whoami() + "pausing renamer")
            pipes["renamer"][0].send(("pause", None, None))
        except Exception as e:
            logger.warning(whoami() + str(e))
    # 9. stop post-proc
    try:
        if mpp["post"]:
            mpid = mpp["post"].pid
            logger.debug(whoami() + "terminating postprocesspr")
            os.kill(mpid, signal.SIGTERM)
            mpp["post"].join()
            mpp["post"] = None
            logger.info(whoami() + "postprocessor terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 10. stop nzbparser
    if stopall:
        try:
            if mpp["nzbparser"]:
                mpid = mpp["nzbparser"].pid
                logger.debug(whoami() + "terminating nzb_parser")
                os.kill(mpp["nzbparser"].pid, signal.SIGTERM)
                mpp["nzbparser"].join()
                mpp["nzbparser"] = None
                logger.info(whoami() + "postprocessor terminated!")
        except Exception as e:
            logger.debug(whoami() + str(e))
    # 11. threads + servers
    if stopall:
        logger.debug(whoami() + "checking termination of connection threads")
        ct.stop_threads()

    logger.info(whoami() + "clearing finished")
    return


def connection_thread_health(threads):
        nothreads = len([t for t, _ in threads])
        nodownthreads = len([t for t, _ in threads if t.connectionstate == -1])
        if nothreads == 0:
            return 0
        return 1 - nodownthreads / (nothreads)


def set_guiconnector_data(guiconnector, results, ct, dl, statusmsg, logger):
    try:
        nzbname, downloaddata, return_reason, maindir = results
        bytescount0, availmem0, avgmiblist, filetypecounter, _, article_health, overall_size, already_downloaded_size, p2,\
            overall_size_wparvol, allfileslist = downloaddata
        downloaddata_gc = bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health, overall_size, already_downloaded_size
        # no ct.servers object if connection idle
        if not ct.servers:
            serverconfig = None
        else:
            serverconfig = ct.servers.server_config
        guiconnector.set_data(downloaddata_gc, ct.threads, serverconfig, statusmsg, dl.serverhealth())
    except Exception as e:
        logger.debug(whoami() + str(e) + ": cannot interpret gui-data from downloader")
    return article_health


# main loop for ginzibix downloader
def ginzi_main(cfg, dirs, subdirs, mp_loggerqueue):

    setproctitle("gzbx." + os.path.basename(__file__))

    logger = setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")

    nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2 = (None, ) * 6

    pwdb = PWDBSender()

    pwdb.exc("db_status_init", [], {})

    # multiprocessing events
    mp_events = {}
    mp_events["unrarer"] = mp.Event()
    mp_events["verifier"] = mp.Event()
    mp_events["post"] = mp.Event()

    # threading events
    event_stopped = threading.Event()

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

    ct = ConnectionThreads(cfg, articlequeue, resultqueue, logger)

    # init sighandler
    logger.debug(whoami() + "initializing sighandler")
    mpp = {"nzbparser": None, "decoder": None, "unrarer": None, "renamer": None, "verifier": None, "post": None}
    sh = SigHandler_Main(event_stopped, logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    # start nzb parser mpp
    logger.debug(whoami() + "starting nzbparser process ...")
    mpp_nzbparser = mp.Process(target=ParseNZB, args=(cfg, dirs, mp_loggerqueue, ))
    mpp_nzbparser.start()
    mpp["nzbparser"] = mpp_nzbparser

    # start renamer
    logger.debug(whoami() + "starting renamer process ...")
    mpp_renamer = mp.Process(target=renamer, args=(renamer_child_pipe, renamer_result_queue, mp_loggerqueue, ))
    mpp_renamer.start()
    mpp["renamer"] = mpp_renamer

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

    # main looooooooooooooooooooooooooooooooooooooooooooooooooooop
    while not event_stopped.wait(0.25):

        # closeall command
        if guiconnector.all_closed():
            logger.info(whoami() + "got closeall")
            event_stopped.set()
            continue

        # set connection health
        if dl:
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
            connection_health = connection_thread_health(ct.threads)
        else:
            article_health = 0
            connection_health = 0
        guiconnector.set_health(article_health, connection_health)

        # has first nzb changed during download? -> stop & start new NZB
        if guiconnector.has_first_nzb_changed() and dl:
            # only reordered
            logger.info(whoami() + "NZBs have been reordered")
            ct.pause_threads()
            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False)
            dl.stop()
            dl.join()
            pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
            # current download nzb has been deleted!
            if guiconnector.has_nzb_been_deleted():
                logger.info(whoami() + "NZBs have been deleted")
                pwdb.exc("db_msg_insert", [nzbname, "NZB(s) deleted", "warning"], {})
                if dl.results:
                    article_health = set_guiconnector_data(guiconnector, dl.results, ct, dl, statusmsg, logger)
                    time.sleep(1)    # there must be a better solution
                deleted_nzb_name0 = guiconnector.has_nzb_been_deleted(delete=True)
                remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger)
            pwdb.exc("store_sorted_nzbs", [], {})
            nzbname = None
            del dl
            dl = None
        # has NZB order changed
        elif guiconnector.has_order_changed():
            pwdb.exc("store_sorted_nzbs", [], {})

        # if not downloading
        if not dl:
            nzbname = make_allfilelist_wait(pwdb, dirs, guiconnector, logger, -1)
            guiconnector.clear_data()
            if nzbname:
                ct.reset_timestamps_bdl()
                logger.info(whoami() + "got next NZB: " + str(nzbname))
                dl = Downloader(cfg, dirs, ct, mp_work_queue, mpp, guiconnector, pipes, renamer_result_queue, mp_events, nzbname, mp_loggerqueue, logger)
                dl.start()
            else:
                time.sleep(0.5)
                continue
        else:
            if dl.results:
                article_health = set_guiconnector_data(guiconnector, dl.results, ct, dl, statusmsg, logger)
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
                                                                    dl.download_dir, dl.dirs, dl.pw_file, mp_events["post"], mp_loggerqueue, ))
                mpp_post.start()
                mpp["post"] = mpp_post
            # if download failed
            elif stat0 == -2:
                logger.info(whoami() + "download failed for NZB " + nzbname)
                ct.pause_threads()
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False)
                dl.stop()
                dl.join()
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                # set 'flags' for getting next nzb
                del dl
                dl = None
                nzbname = None
                pwdb.exc("store_sorted_nzbs", [], {})
            # if postproc ok
            elif stat0 == 4:
                logger.info(whoami() + "postprocessor success for NZB " + nzbname)
                ct.pause_threads()
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False)
                dl.stop()
                dl.join()
                if mpp["post"]:
                    mpp["post"].join()
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                article_health = set_guiconnector_data(guiconnector, dl.results, ct, dl, "success", logger)
                pwdb.exc("db_msg_insert", [nzbname, "downloaded and postprocessed successfully!", "success"], {})
                mpp["post"] = None
                # set 'flags' for getting next nzb
                del dl
                dl = None
                nzbname = None
                pwdb.exc("store_sorted_nzbs", [], {})
            # if postproc failed
            elif stat0 == -4:
                logger.error(whoami() + "postprocessor failed for NZB " + nzbname)
                ct.pause_threads()
                clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, logger, stopall=False)
                dl.stop()
                dl.join()
                if mpp["post"]:
                    mpp["post"].join()
                pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
                article_health = set_guiconnector_data(guiconnector, dl.results, ct, dl, "failed", logger)
                pwdb.exc("db_msg_insert", [nzbname, "downloaded and/or postprocessing failed!", "error"], {})
                mpp["post"] = None
                # set 'flags' for getting next nzb
                del dl
                dl = None
                nzbname = None
                pwdb.exc("store_sorted_nzbs", [], {})

    # shutdown
    logger.info(whoami() + "closeall: starting shutdown sequence")
    ct.pause_threads()
    logger.debug(whoami() + "closeall: connection threads paused")
    if dl:
        dl.stop()
        dl.join()
    logger.debug(whoami() + "closeall: downloader joined")
    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=True, onlyarticlequeue=False)
    dl = None
    logger.debug(whoami() + "closeall: all cleared")
    pwdb.exc("db_nzb_store_allfile_list", [nzbname, allfileslist, filetypecounter, overall_size, overall_size_wparvol, p2], {})
    logger.info(whoami() + "exited!")
