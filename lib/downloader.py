import queue
import re
import pickle
import shutil
import os
import time
import threading
import multiprocessing as mp
from threading import Thread
from .aux import PWDBSender, mpp_is_alive
from .mplogging import whoami
from .par_verifier import par_verifier
from .par2lib import Par2File
from .partial_unrar import partial_unrar
import psutil
import sys
from .article_decoder import decode_articles
from .passworded_rars import is_rar_password_protected


# Handles download of a NZB file
class Downloader(Thread):
    def __init__(self, cfg, dirs, ct, mp_work_queue, mpp, guiconnector, pipes, renamer_result_queue, mp_events,
                 nzbname, mp_loggerqueue, aqlock, logger):
        Thread.__init__(self)
        self.daemon = True
        self.lock = threading.Lock()
        self.aqlock = aqlock
        self.allfileslist, self.filetypecounter, self.overall_size, self.overall_size_wparvol, self.p2 = (None, ) * 5
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
        articlelist = []
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
                    try:
                        infolist[filename]
                    except Exception:
                        infolist[filename] = [None] * nr_articles
                    self.pwdb.exc("db_file_update_status", [filename, 1], {})   # status do downloading
                    for i, art0 in enumerate(file_articles):
                        if i == 0:
                            continue
                        art_nr, art_name, art_bytescount = art0
                        try:
                            art_downloaded = True if infolist[filename][art_nr - 1] is not None else False
                        except Exception:
                            art_downloaded = False
                        if not art_downloaded:
                            bytescount0 += art_bytescount
                            articlelist.append((filename, age, filetype, nr_articles, art_nr, art_name,
                                                level_servers))
                        article_count += 1
        with self.aqlock:
            for al0 in articlelist:
                self.articlequeue.put(al0)
        bytescount0 = bytescount0 / (1024 * 1024 * 1024)
        return files, infolist, bytescount0, article_count

    def all_queues_are_empty(self):
        articlequeue_empty = self.articlequeue.qsize() > 0
        resultqueue_empty = self.resultqueue.qsize() > 0
        mpworkqueue_empty = self.mp_work_queue.qsize() > 0
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
        updatedlist = []
        while True:
            try:
                resultarticle = self.resultqueue.get_nowait()
                self.resultqueue.task_done()
                filename, age, filetype, nr_articles, art_nr, art_name, download_server, inf0, add_bytes = resultarticle
                updatedlist.append(art_name)
                if inf0 == 0:
                    continue
                elif inf0 == "failed":
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
                if not f_done and len([inf for inf in infolist[filename] if inf]) == f_nr_articles:
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
        self.pwdb.exc("db_article_set_status", [updatedlist, 1], {})
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

        # def download(self, nzbname, servers_shut_down):
        nzbname = self.nzbname
        return_reason = None
        article_count = 0

        res = self.pwdb.exc("db_nzb_get_allfile_list", [nzbname], {})
        self.allfileslist, self.filetypecounter, self.overall_size, self.overall_size_wparvol, self.already_downloaded_size, self.p2 = res

        bytescount0 = self.getbytescount(self.allfileslist)
        sanity0 = -1
        infolist = {}

        # read infolist
        try:
            nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
            fn = self.dirs["incomplete"] + nzbdir + "infl_" + nzbname + ".gzbx"
            with open(fn, "rb") as fp:
                infolist = pickle.load(fp)
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ": cannot load infolist from file")
            infolist = {}

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
        loadpar2vols = False
        availmem0 = psutil.virtual_memory()[0] - psutil.virtual_memory()[1]
        bytescount0 = 0
        article_health = 1
        self.ct.reset_timestamps()
        if self.pwdb.exc("db_nzb_getstatus", [nzbname], {}) > 2:
            self.logger.info(nzbname + "- download complete!")
            self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                      self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                      self.allfileslist)), "download complete", self.main_dir
            sys.exit()

        # make dirs
        self.logger.debug(whoami() + "creating dirs")
        self.make_dirs(nzbname)

        # trigger set_data for gui
        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                  self.allfileslist)), "N/A", self.main_dir
        self.pwdb.exc("db_nzb_update_status", [nzbname, 2], {})    # status "downloading"

        if self.filetypecounter["par2vol"]["max"] > 0:
            self.contains_par_files = True

        # which set of filetypes should I download
        self.logger.debug(whoami() + "download: define inject set")
        if self.filetypecounter["par2"]["max"] > 0 and self.filetypecounter["par2"]["max"] > self.filetypecounter["par2"]["counter"]:
            inject_set0 = ["par2"]
        elif self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            inject_set0 = ["etc", "par2vol", "rar", "sfv", "nfo"]
            loadpar2vols = True
        else:
            inject_set0 = ["etc", "sfv", "nfo", "rar"]
        self.logger.info(whoami() + "Overall_Size: " + str(self.overall_size) + ", incl. par2vols: " + str(self.overall_size_wparvol))

        # check if paused!
        if self.event_paused.isSet():
            self.logger.debug(whoami() + "paused, waiting for resume before starting downloader")
            while self.event_paused.wait(0.1):
                if self.event_stopped.wait(0.1):
                    break

        if not self.event_stopped.isSet():
            self.pipes["renamer"][0].send(("start", self.download_dir, self.rename_dir))

            # sanity check
            inject_set_sanity = []
            if self.cfg["OPTIONS"]["SANITY_CHECK"].lower() == "yes" and not self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
                self.pwdb.exc("db_msg_insert", [nzbname, "checking for sanity", "info"], {})
                sanity0 = self.do_sanity_check(self.allfileslist, files, infolist, bytescount0, self.filetypecounter)
                if sanity0 < 1:
                    if (self.filetypecounter["par2vol"]["max"] > 0 and sanity0 < self.crit_art_health_w_par) or\
                       (self.filetypecounter["par2vol"]["max"] == 0 and sanity0 < self.crit_art_health_wo_par):
                        self.pwdb.exc("db_msg_insert", [nzbname, "Sanity less than criticical health level, exiting", "error"], {})
                        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                                  self.allfileslist)), "download failed", self.main_dir
                        sys.exit()

                    self.pwdb.exc("db_msg_insert", [nzbname, "Sanity less than 100%, preloading par2vols!", "warning"], {})
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    self.overall_size = self.overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set_sanity = ["par2vol"]
                    self.ct.pause_threads()
                    self.clear_queues_and_pipes()
                    time.sleep(0.5)
                    self.ct.resume_threads()
                else:
                    self.pwdb.exc("db_msg_insert", [nzbname, "Sanity is 100%, all OK!", "info"], {})

            # start decoder mpp
            self.logger.debug(whoami() + "starting decoder process ...")
            self.mpp_decoder = mp.Process(target=decode_articles, args=(self.mp_work_queue, self.mp_loggerqueue, ))
            self.mpp_decoder.start()
            self.mpp["decoder"] = self.mpp_decoder

            # inject articles and GO!
            files, infolist, bytescount0, article_count = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                               self.filetypecounter)

            # inject par2vols because of sanity check
            if inject_set_sanity:
                files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set_sanity, self.allfileslist, files, infolist, bytescount0,
                                                                                     self.filetypecounter)
                bytescount0 += bytescount00
                article_count += article_count0

        # reset bytesdownloaded
        self.ct.reset_timestamps()

        # download loop until articles downloaded
        oldrarcounter = 0
        self.pwdb.exc("db_msg_insert", [nzbname, "downloading", "info"], {})

        stopped_max_counter = 5
        getnextnzb = False
        article_failed = 0

        while self.stopped_counter < stopped_max_counter:

            # print(time.time(), "#1")

            # if terminated: ensure that all tasks are processed
            if self.event_stopped.wait(0.25):
                self.stopped_counter += 1
                self.resume()
                self.logger.info(whoami() + "termination countdown: " + str(self.stopped_counter) + " of " + str(stopped_max_counter))
                time.sleep(0.25)

            return_reason = None

            if self.event_paused.isSet():
                continue

            self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                      self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                      self.allfileslist)), "N/A", self.main_dir

            # if dl is finished
            if getnextnzb:
                self.logger.info(nzbname + "- download success!")
                return_reason = "download success"
                self.pwdb.exc("db_nzb_update_status", [nzbname, 3], {})
                self.stop()
                self.stopped_counter = stopped_max_counter       # stop immediately
                continue

            # print(time.time(), "#2")

            # check if process has died somehow and restart in case
            if self.mpp["unrarer"] and not mpp_is_alive(self.mpp, "unrarer"):
                self.logger.debug(whoami() + "restarting unrarer process ...")
                self.mpp_unrarer = mp.Process(target=partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                          nzbname, self.mp_loggerqueue, None, self.event_unrareridle, self.cfg, ))
                self.mpp_unrarer.start()
                self.mpp["unrarer"] = self.mpp_unrarer
            if self.mpp["decoder"] and not mpp_is_alive(self.mpp, "decoder"):
                self.logger.debug(whoami() + "restarting decoder process ...")
                self.mpp_decoder = mp.Process(target=decode_articles, args=(self.mp_work_queue, self.mp_loggerqueue, ))
                self.mpp_decoder.start()
                self.mpp["decoder"] = self.mpp_decoder
            # verifier is checked anyway below

            # get renamer_result_queue (renamer.py)
            while not self.event_stopped.isSet():
                try:
                    filename, full_filename, filetype, old_filename, old_filetype = self.renamer_result_queue.get_nowait()
                    self.pwdb.exc("db_msg_insert", [nzbname, "downloaded & renamed " + filename, "info"], {})
                    # have files been renamed ?
                    if old_filename != filename or old_filetype != filetype:
                        self.logger.info(whoami() + old_filename + "/" + old_filetype + " changed to " + filename + " / " + filetype)
                        # update filetypecounter
                        self.filetypecounter[old_filetype]["filelist"].remove(old_filename)
                        self.filetypecounter[filetype]["filelist"].append(filename)
                        self.filetypecounter[old_filetype]["max"] -= 1
                        self.filetypecounter[filetype]["counter"] += 1
                        self.filetypecounter[filetype]["max"] += 1
                        self.filetypecounter[filetype]["loadedfiles"].append(filename)
                        # update allfileslist
                        for i, o_lists in enumerate(self.allfileslist):
                            o_orig_name, o_age, o_type, o_nr_articles = o_lists[0]
                            if o_orig_name == old_filename:
                                self.allfileslist[i][0] = (filename, o_age, o_type, o_nr_articles)
                    else:
                        self.logger.debug(whoami() + "moved " + filename + " to renamed dir")
                        self.filetypecounter[filetype]["counter"] += 1
                        self.filetypecounter[filetype]["loadedfiles"].append(filename)
                    if (filetype == "par2" or filetype == "par2vol") and not self.p2:
                        self.p2 = Par2File(full_filename)
                        self.logger.info(whoami() + "found first par2 file")
                    if inject_set0 == ["par2"] and (filetype == "par2" or self.filetypecounter["par2"]["max"] == 0):
                        # print(time.time(), filename, filetype, self.p2)
                        self.logger.debug(whoami() + "injecting rars etc.")
                        inject_set0 = ["etc", "sfv", "nfo", "rar"]
                        files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                                             self.filetypecounter)
                        bytescount0 += bytescount00
                        article_count += article_count0
                except (queue.Empty, EOFError):
                    break
                except Exception:
                    break

            # print(time.time(), "#3")
            # if verifier tells us so, we load par2vol files
            if not self.event_stopped.isSet() and not loadpar2vols:
                if self.pipes["verifier"][0].poll():
                    loadpar2vols = self.pipes["verifier"][0].recv()
                if loadpar2vols:
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar(s) corrupt, loading par2vol files", "info"], {})
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    self.overall_size = self.overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                                         self.filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0

            # check if unrarer is dead due to wrong rar on start
            if mpp_is_alive(self.mpp, "unrarer") and self.pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) == -2:
                self.mpp["unrarer"].join()
                self.mpp["unrarer"] = None
                self.logger.debug(whoami() + "unrarer joined")

            # print(time.time(), "#4")
            if mpp_is_alive(self.mpp, "verifier"):
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

            if not self.event_stopped.isSet() and not mpp_is_alive(self.mpp, "unrarer") and self.filetypecounter["rar"]["counter"] > oldrarcounter\
               and not self.pwdb.exc("db_nzb_get_ispw_checked", [nzbname], {}):
                # testing if pw protected
                rf = [rf0 for _, _, rf0 in os.walk(self.verifiedrar_dir) if rf0]
                # if no rar files in verified_rardir: skip as we cannot test for password
                if rf:
                    oldrarcounter = self.filetypecounter["rar"]["counter"]
                    self.logger.debug(whoami() + ": first/new verified rar file appeared, testing if pw protected")
                    is_pwp = is_rar_password_protected(self.verifiedrar_dir, self.logger)
                    self.pwdb.exc("db_nzb_set_ispw_checked", [nzbname, True], {})
                    if is_pwp in [0, -2]:
                        self.logger.warning(whoami() + "cannot test rar if pw protected, something is wrong: " + str(is_pwp) + ", exiting ...")
                        self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})  # status download failed
                        return_reason = "download failed"
                        self.pwdb.exc("db_msg_insert", [nzbname, "download failed due to pw test not possible", "error"], {})
                        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                                  self.allfileslist)), return_reason, self.main_dir
                        self.stop()
                        self.stopped_counter = stopped_max_counter
                        continue
                    if is_pwp == 1:
                        # if pw protected -> postpone password test + unrar
                        self.pwdb.exc("db_nzb_set_ispw", [nzbname, True], {})
                        self.pwdb.exc("db_msg_insert", [nzbname, "rar archive is password protected", "warning"], {})
                        self.logger.info(whoami() + "rar archive is pw protected, postponing unrar to postprocess ...")
                    elif is_pwp == -1:
                        # if not pw protected -> normal unrar
                        self.logger.info(whoami() + "rar archive is not pw protected, starting unrarer ...")
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

            # print(time.time(), "#5")
            # if par2 available start par2verifier, else just copy rars unchecked!
            if not self.event_stopped.isSet() and not mpp_is_alive(self.mpp, "verifier"):
                all_rars_are_verified, _ = self.pwdb.exc("db_only_verified_rars", [nzbname], {})
                if not all_rars_are_verified:
                    pvmode = None
                    if self.p2:
                        pvmode = "verify"
                    elif not self.p2 and self.filetypecounter["par2"]["max"] == 0:
                        pvmode = "copy"
                    if pvmode:
                        self.logger.debug(whoami() + "starting rar_verifier process (mode=" + pvmode + ")for NZB " + nzbname)
                        self.mpp_verifier = mp.Process(target=par_verifier, args=(self.pipes["verifier"][1], self.rename_dir, self.verifiedrar_dir,
                                                                                  self.main_dir, self.mp_loggerqueue, nzbname, pvmode, self.event_verifieridle,
                                                                                  self.cfg, ))
                        self.mpp_verifier.start()
                        self.mpp["verifier"] = self.mpp_verifier
                        self.logger.info(whoami() + "verifier started!")

            # print(time.time(), "#6")
            # read resultqueue + decode via mp
            newresult, avgmiblist, infolist, files, failed = self.process_resultqueue(avgmiblist, infolist, files)
            # print(time.time(), "#6a")
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
                if (article_health < self.crit_art_health_wo_par and self.filetypecounter["par2vol"]["max"] == 0) \
                   or par2failed \
                   or (self.filetypecounter["par2vol"]["max"] > 0 and article_health <= self.crit_art_health_w_par):
                    self.logger.info(whoami() + "articles missing and cannot repair, exiting download")
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                    if par2failed:
                        self.pwdb.exc("db_msg_insert", [nzbname, "par2 file broken/not available on servers", "error"], {})
                    else:
                        self.pwdb.exc("db_msg_insert", [nzbname, "critical health threashold exceeded", "error"], {})
                    return_reason = "download failed"
                    self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                              self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                              self.allfileslist)), return_reason, self.main_dir
                    self.stop()
                    self.stopped_counter = stopped_max_counter
                    continue
                if not loadpar2vols and self.filetypecounter["par2vol"]["max"] > 0 and article_health > 0.95:
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                                         self.filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0
                    self.overall_size = self.overall_size_wparvol
                    loadpar2vols = True
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar(s) corrupt, loading par2vol files", "info"], {})

            # print(time.time(), "#7")
            # check if all files are downloaded
            getnextnzb = True
            for filetype, item in self.filetypecounter.items():
                if filetype == "par2vol" and not loadpar2vols:
                    continue
                if self.filetypecounter[filetype]["counter"] < self.filetypecounter[filetype]["max"]:
                    getnextnzb = False
                    break

            # if all files are downloaded and still articles in queue --> inconsistency, exit!
            if getnextnzb and not self.all_queues_are_empty:
                self.pwdb.exc("db_msg_insert", [nzbname, "inconsistency in download queue", "error"], {})
                self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                self.logger.warning(whoami() + ": records say dl is done, but still some articles in queue, exiting ...")
                return_reason = "download failed"
                self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                          self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                          self.allfileslist)), return_reason, self.main_dir
                self.stop()
                self.stopped_counter = stopped_max_counter
                continue
            # print("-" * 60)

        # store infolist in file
        if infolist and files:
            saveinfolist = {}
            try:
                nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
                fn = self.dirs["incomplete"] + nzbdir + "infl_" + nzbname + ".gzbx"
                with open(fn, "wb") as fp:
                    for filename, item in files.items():
                        f_nr_articles, f_age, f_filetype, f_done, f_failed = item
                        if not f_done:
                            saveinfolist[filename] = infolist[filename]
                    pickle.dump(saveinfolist, fp)
            except Exception as e:
                self.logger.warning(whoami() + str(e) + ": cannot write " + fn)

        self.pwdb.exc("db_nzb_store_allfile_list", [self.nzbname, self.allfileslist, self.filetypecounter, self.overall_size,
                                                    self.overall_size_wparvol, self.p2], {})
        # save infolist here
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
        self.results = nzbname, ((bytescount0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                  self.allfileslist)), return_reason, self.main_dir

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