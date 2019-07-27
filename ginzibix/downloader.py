import queue
import re
import pickle
import shutil
import os
import time
import threading
import multiprocessing as mp
from threading import Thread
import psutil
import sys
import ginzyenc

from ginzibix.mplogging import whoami
from ginzibix import par_verifier, par2lib, partial_unrar, article_decoder, passworded_rars
from ginzibix import PWDBSender, mpp_is_alive, mpp_join, GUI_Poller, get_cut_nzbname, get_cut_msg, get_bg_color, get_status_name_and_color,\
    clear_postproc_dirs, get_server_config, get_configured_servers, get_config_for_server, get_free_server_cfg, is_port_in_use, do_mpconnections,\
    kill_mpp


empty_yenc_article = [b"=ybegin line=128 size=14 name=ginzi.txt",
                      b'\x9E\x92\x93\x9D\x4A\x93\x9D\x4A\x8F\x97\x9A\x9E\xA3\x34\x0D\x0A',
                      b"=yend size=14 crc32=8111111c"]

CRIT_ART_HEALTH_W_PAR = 0.98
CRIT_ART_HEALTH_WO_PAR = 0.998
CRIT_ART_HEALTH = 0.80
CRIT_CONN_HEALTH = 0.7


# Handles download of a NZB file
class Downloader(Thread):
    def __init__(self, cfg, dirs, ct, mp_work_queue, articlequeue, resultqueue, mpp, pipes,
                 renamer_result_queue, mp_events, nzbname, mp_loggerqueue, filewrite_lock, logger):
        Thread.__init__(self)
        self.daemon = True
        self.lock = threading.Lock()
        self.filewrite_lock = filewrite_lock
        self.allfileslist, self.filetypecounter, self.overall_size, self.overall_size_wparvol, self.p2 = (None, ) * 5
        self.p2list = []
        self.mp_loggerqueue = mp_loggerqueue
        self.nzbname = nzbname
        self.event_unrareridle = mp_events["unrarer"]
        self.event_verifieridle = mp_events["verifier"]
        self.cfg = cfg
        self.pipes = pipes
        self.pwdb = PWDBSender()
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.mp_work_queue = mp_work_queue
        self.renamer_result_queue = renamer_result_queue
        self.mp_unrarqueue = mp.Queue()
        self.mp_nzbparser_outqueue = mp.Queue()
        self.mp_nzbparser_inqueue = mp.Queue()
        self.ct = ct
        self.dirs = dirs
        self.logger = logger
        self.mpp = mpp
        self.article_health = 1
        self.connection_health = 1
        self.contains_par_files = False
        self.results = None
        self.read_cfg()
        self.event_stopped = threading.Event()
        self.event_paused = threading.Event()
        self.stopped_counter = 0
        self.thread_is_running = False

        self.crit_conn_health = CRIT_CONN_HEALTH
        self.crit_art_health_w_par = CRIT_ART_HEALTH_W_PAR
        self.crit_art_health_wo_par = CRIT_ART_HEALTH_WO_PAR
        self.crit_art_health = CRIT_ART_HEALTH

        if self.pw_file:
            try:
                self.logger.debug(whoami() + "as a first test, open password file")
                with open(self.pw_file, "r") as f0:
                    f0.readlines()
                self.logger.info(whoami() + "password file is available")
            except Exception as e:
                self.logger.warning(whoami() + str(e) + ": cannot open pw file, setting to None")
                self.pw_file = None

        # make dirs
        self.logger.debug(whoami() + "creating dirs")
        self.make_dirs(nzbname)

    def stop(self):
        self.event_stopped.set()
        if not self.thread_is_running:
            self.start()
        else:
            with self.lock:
                self.stopped_counter = 0

    def pause(self):
        self.event_paused.set()

    def resume(self):
        self.event_paused.clear()

    def read_cfg(self):
        # pw_file
        try:
            self.pw_file = self.cfg["OPTIONS"]["PW_FILE"]
            self.logger.debug(whoami() + "password file is: " + self.pw_file)
        except Exception as e:
            self.logger.debug(whoami() + str(e) + ": no pw file provided!")
            self.pw_file = None
        # connection_idle_timeout
        try:
            self.connection_idle_timeout = float(self.cfg["OPTIONS"]["CONNECTION_IDLE_TIMEOUT"])
            self.logger.debug(whoami() + "connection_idle_timeout is: " + str(self.connection_idle_timeout))
        except Exception as e:
            self.logger.debug(whoami() + str(e) + ": setting connection_idle_timeout to 15 sec.")
            self.connection_idle_timeout = 30

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

    def inject_articles(self, ftypes, filelist, files0, infolist0, bytescount0_0, filetypecounter, onlyfirstarticle=False):
        # generate all articles and files
        files = files0
        infolist = infolist0
        bytescount0 = bytescount0_0
        article_count = 0
        entire_artqueue = []
        for f in ftypes:
            for j, file_articles in enumerate(reversed(filelist)):
                # iterate over all articles in file
                filename, age, filetype, nr_articles = file_articles[0]
                if onlyfirstarticle:
                    filestatus = -100
                else:
                    filestatus = self.pwdb.exc("db_file_getstatus", [filename], {})
                # reconcile filetypecounter with db
                if filetype == f and filestatus != -100:
                    if filename in filetypecounter[f]["filelist"] and filename not in filetypecounter[f]["loadedfiles"] and filestatus not in [0, 1]:
                        filetypecounter[f]["counter"] += 1
                        filetypecounter[f]["loadedfiles"].append(filename)
                if filetype == f and (filestatus in [0, 1, -100]):
                    level_servers = self.get_level_servers(age)
                    files[filename] = (nr_articles, age, filetype, False, True)
                    if not onlyfirstarticle:
                        try:
                            infolist[filename]
                        except Exception:
                            infolist[filename] = [None] * nr_articles
                    if not onlyfirstarticle:
                        self.pwdb.exc("db_file_update_status", [filename, 1], {})   # status do downloading
                    for i, art0 in enumerate(file_articles):
                        if i == 0:
                            continue
                        art_nr, art_name, art_bytescount = art0
                        if not onlyfirstarticle:
                            try:
                                art_downloaded = True if infolist[filename][art_nr - 1] is not None else False
                            except Exception:
                                art_downloaded = False
                            if not art_downloaded:
                                bytescount0 += art_bytescount
                                entire_artqueue.append((filename, age, filetype, nr_articles, art_nr, art_name,
                                                        level_servers))
                        else:
                            if art_nr == 1:
                                entire_artqueue.append((filename, age, filetype, nr_articles, art_nr, art_name,
                                                        level_servers))
                                break
                        article_count += 1
        if entire_artqueue:
            do_mpconnections(self.pipes, "push_entire_articlequeue", entire_artqueue)
        bytescount0 = bytescount0 / (1024 * 1024 * 1024)
        return files, infolist, bytescount0, article_count

    def all_queues_are_empty(self):
        articlequeue_empty = resultqueue_empty = do_mpconnections(self.pipes, "queues_empty", None)
        mpworkqueue_empty = self.mp_work_queue.qsize() == 0
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

        resultlist = do_mpconnections(self.pipes, "pull_entire_resultqueue", None)
        ri = 0
        len_resultlist = 0
        if resultlist:
            try:
                len_resultlist = len(resultlist)
            except Exception:
                pass

        while True:
            try:
                if ri >= len_resultlist:
                    break
                # resultarticle = do_mpconnections(self.pipes, "pull_resultqueue", None) 
                resultarticle = resultlist[ri]
                ri += 1
                if not resultarticle:
                    break
                try:
                    filename, age, filetype, nr_articles, art_nr, art_name, download_server, inf0, add_bytes = resultarticle
                except Exception:
                    break

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
                    except Exception:
                        bytesdownloaded = 0
                    avgmiblist.append((time.time(), bytesdownloaded, download_server))
                try:
                    infolist[filename][art_nr - 1] = inf0
                    newresult = True
                except TypeError:
                    continue
                self.allbytesdownloaded0 += bytesdownloaded
                # check if file is completed and put to mp_queue/decode in case
                (f_nr_articles, f_age, f_filetype, f_done, f_failed) = files[filename]
                len_articles_done = len([inf for inf in infolist[filename] if inf])
                if not f_done and len_articles_done == f_nr_articles:
                    failed0 = False
                    if b"name=ginzi.txt" in infolist[filename][0][0]:
                        failed0 = True
                    inflist0 = infolist[filename][:]
                    self.mp_work_queue.put((inflist0, self.download_dir, filename, filetype))
                    files[filename] = (f_nr_articles, f_age, f_filetype, True, failed0)
                    infolist[filename] = None
                    self.logger.debug(whoami() + "All articles for " + filename + " downloaded, calling mp.decode ...")
            except IndexError:
                break
            except KeyError:
                pass
        if len(avgmiblist) > 50:
            avgmiblist = avgmiblist[:50]
        self.pwdb.exc("db_article_set_status", [updatedlist, 1], {})
        return newresult, avgmiblist, infolist, files, failed

    def clear_queues_and_pipes(self, onlyarticlequeue=False):
        self.logger.debug(whoami() + "starting clearing queues & pipes")
        do_mpconnections(self.pipes, "clear_articlequeue", None)
        if onlyarticlequeue:
            return True
        return do_mpconnections(self.pipes, "clear_resultqueue", None)
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
        do_mpconnections(self.pipes, "set_tmode_sanitycheck", None)
        sanity_injects = ["rar", "sfv", "nfo", "etc", "par2"]
        files, infolist, bytescount0, article_count = self.inject_articles(sanity_injects, allfileslist, files, infolist, bytescount0, filetypecounter)
        artsize0 = do_mpconnections(self.pipes, "len_articlequeue", None)
        self.logger.info(whoami() + "Checking sanity on " + str(artsize0) + " articles")
        do_mpconnections(self.pipes, "clear_articlequeue", None)
        nr_articles = 0
        nr_ok_articles = 0
        while True:
            try:
                resultarticle = do_mpconnections(self.pipes, "pull_resultqueue", None)
                if not resultarticle:
                    break
                nr_articles += 1
                _, _, _, _, _, artname, _, status, _ = resultarticle
                if status != "failed":
                    nr_ok_articles += 1
                else:
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "cannot download " + artname, "info"], {})
                    self.logger.debug(whoami() + "cannot download " + artname)
            except (queue.Empty, EOFError, IndexError):
                break
        if nr_articles == 0:
            a_health = 0
        else:
            a_health = nr_ok_articles / (nr_articles)
        self.logger.info(whoami() + "article health: {0:.4f}".format(a_health * 100) + "%")
        do_mpconnections(self.pipes, "set_tmode_download", None)
        return a_health

    def do_pre_analyze(self):
        res = self.pwdb.exc("db_nzb_get_allfile_list", [self.nzbname], {})
        self.allfileslist, self.filetypecounter, self.overall_size, self.overall_size_wparvol, self.already_downloaded_size, self.p2 = res
        # hier noch:
        #   - filetype ändern
        self.pwdb.exc("db_msg_insert", [self.nzbname, "Performing NZB pre-analysis for obfusction detection", "info"], {})
        self.logger.info(whoami() + "performing pre-analysis")
        do_mpconnections(self.pipes, "set_tmode_download", None)
        injects = ["rar", "sfv", "nfo", "etc", "par2", "par2vol"]
        files, infolist, bytescount0, article_count = self.inject_articles(injects, self.allfileslist, {}, {}, 0,
                                                                           self.filetypecounter, onlyfirstarticle=True)
        artsize0 = do_mpconnections(self.pipes, "len_articlequeue", None)
        self.logger.info(whoami() + "Pre-Analyzing on " + str(artsize0) + " articles")
        nr_articles = 0
        obfusc_detected = False
        status = 1
        while not self.event_stopped.isSet():
            try:
                resultarticle = do_mpconnections(self.pipes, "pull_resultqueue", None)
                if not resultarticle:
                    continue
                filename, age, old_filetype, _, art_nr, _, status_article, inf0, _ = resultarticle
                if status_article == "failed":
                    status = -1
                    continue
                ftype = old_filetype
                try:
                    lastline = inf0[-1].decode("latin-1")
                    m = re.search('size=(.\d+?) ', lastline)
                    if m:
                        size = int(m.group(1))
                except Exception as e:
                    self.logger.warning(whoami() + str(e) + ", guestimate size ...")
                    size = int(sum(len(i) for i in inf0.lines) * 1.1)
                try:
                    decoded, _, _, _, _ = ginzyenc.decode_usenet_chunks(inf0, size)
                except Exception:
                    continue
                if decoded[:4] == b"Rar!":
                    ftype = "rar"
                else:
                    bstr0 = b"PAR2\x00"             # PAR2\x00
                    bstr1 = b"PAR 2.0\x00FileDesc"  # PAR 2.0\x00FileDesc'
                    if bstr0 in decoded and bstr1 in decoded:
                        ftype = "par2"
                    elif bstr0 in decoded and bstr1 not in decoded:
                        ftype = "par2vol"
                if ftype != old_filetype:
                    obfusc_detected = True
                    self.logger.info(whoami() + filename + ": setting type from " + old_filetype + " to " + ftype)
                    self.pwdb.exc("db_file_set_file_type", [filename, ftype], {})
                    # update filetypecounter
                    self.filetypecounter[old_filetype]["filelist"].remove(filename)
                    self.filetypecounter[ftype]["filelist"].append(filename)
                    self.filetypecounter[old_filetype]["max"] -= 1
                    self.filetypecounter[ftype]["max"] += 1
                    # update allfileslist
                    for i, o_lists in enumerate(self.allfileslist):
                        o_orig_name, o_age, o_type, o_nr_articles = o_lists[0]
                        if o_orig_name == filename:
                            self.allfileslist[i][0] = (filename, o_age, ftype, o_nr_articles)
                            break
                    # overall_size_wparvol
                    if old_filetype == "par2vol" or ftype == "par2vol":
                        fsize = self.pwdb.exc("db_file_getsize", [filename], {})
                        if ftype == "par2vol":
                            self.overall_size_wparvol += fsize
                        if old_filetype == "par2vol":
                            self.overall_size_wparvol -= fsize
                nr_articles += 1
                if do_mpconnections(self.pipes, "queues_empty", None):
                    break
            except (queue.Empty, EOFError, IndexError):
                continue
        do_mpconnections(self.pipes, "clear_articlequeue", None)
        do_mpconnections(self.pipes, "clear_resultqueue", None)
        if self.event_stopped.isSet():
            return 0
        self.pwdb.exc("db_nzb_set_preanalysis_status", [self.nzbname, 1], {})
        if obfusc_detected:
            self.pwdb.exc("db_nzb_store_allfile_list", [self.nzbname, self.allfileslist, self.filetypecounter, self.overall_size,
                                                        self.overall_size_wparvol, self.p2], {})
            self.pwdb.exc("db_msg_insert", [self.nzbname, "Obfuscations detected - setting new file types", "info"], {})
            self.logger.warning(whoami() + "Obfuscations detected - setting new file types")

            self.allfileslist, self.filetypecounter, self.overall_size, self.overall_size_wparvol, self.already_downloaded_size, self.p2 = self.pwdb.exc(
                "db_nzb_get_allfile_list", [self.nzbname], {})
        else:
            self.pwdb.exc("db_msg_insert", [self.nzbname, "No obfuscations detected!", "info"], {})
            self.logger.warning(whoami() + "No obfuscations detected!")
        if status == -1:
            self.logger.warning(whoami() + "Not all files could be checked for obfuscation, articles missing ...")
            self.pwdb.exc("db_msg_insert", [self.nzbname, "Not all files could be checked for obfuscation, articles missing ...", "warning"], {})
        return status

    # main download routine
    def run(self):

        # this is necessary: if not-yet-started downloader is stopped & joined externally,
        #                    run() has to be called once, but be left also immediately!!!
        if self.event_stopped.isSet():
            sys.exit()

        self.thread_is_running = True

        # def download(self, nzbname, servers_shut_down):
        self.allbytesdownloaded0 = 0
        nzbname = self.nzbname
        return_reason = None
        article_count = 0

        # if preanalysed:
        if self.pwdb.exc("db_nzb_get_preanalysis_status", [nzbname], {}) == 1:
            self.allfileslist, self.filetypecounter, self.overall_size, self.overall_size_wparvol, self.already_downloaded_size, self.p2 = self.pwdb.exc(
                "db_nzb_get_allfile_list", [nzbname], {})
        else:
            # do pre analysis
            self.do_pre_analyze()
            if self.event_stopped.isSet():
                sys.exit()

        bytescount0 = self.getbytescount(self.allfileslist)
        sanity0 = -1
        infolist = {}

        # get list of par2 files
        self.p2list = self.pwdb.exc("db_p2_get_p2list", [self.nzbname], {})

        # read infolist
        try:
            nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
            fn = self.dirs["incomplete"] + nzbdir + "infl_" + nzbname + ".gzbx"
            with open(fn, "rb") as fp:
                infolist = pickle.load(fp)
            self.allbytesdownloaded0 = int(self.already_downloaded_size * (1024 * 1024 * 1024))
            # force recheck of password in order to restart unrarer
            self.pwdb.exc("db_nzb_set_ispw_checked", [nzbname, False], {})
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
        inject_set0 = ["par2", "sfv"]             # par2 first!!
        files = {}
        loadpar2vols = False
        availmem0 = psutil.virtual_memory()[0] - psutil.virtual_memory()[1]
        bytescount0 = 0
        article_health = 1

        do_mpconnections(self.pipes, "reset_timestamps", None)

        startstatus = self.pwdb.exc("db_nzb_getstatus", [nzbname], {})
        self.logger.debug(whoami() + "nzb has status " + str(startstatus))

        if startstatus > 2:
            self.logger.info(whoami() + nzbname + "- download complete!")
            self.results = nzbname, ((bytescount0, 0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                      self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                      self.allfileslist)), "download complete", self.main_dir
            sys.exit()

        # trigger set_data for gui
        self.results = nzbname, ((bytescount0, 0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                  self.allfileslist)), "N/A", self.main_dir
        self.pwdb.exc("db_nzb_update_status", [nzbname, 2], {})    # status "downloading"

        if self.filetypecounter["par2vol"]["max"] > 0:
            self.contains_par_files = True

        # which set of filetypes should I download
        self.logger.debug(whoami() + "download: define inject set")
        if (self.filetypecounter["par2"]["max"] > 0 and self.filetypecounter["par2"]["max"] > self.filetypecounter["par2"]["counter"]) or\
           (self.filetypecounter["sfv"]["max"] > 0 and self.filetypecounter["sfv"]["max"] > self.filetypecounter["sfv"]["counter"]):
            inject_set0 = ["par2", "sfv"]
        elif self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
            inject_set0 = ["etc", "par2vol", "rar", "nfo", "sfv"]
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

            self.pipes["renamer"][0].send(("start", self.download_dir, self.rename_dir, nzbname))

            # sanity check
            inject_set_sanity = []
            if self.cfg["OPTIONS"]["SANITY_CHECK"].lower() == "yes" and not self.pwdb.exc("db_nzb_loadpar2vols", [nzbname], {}):
                self.pwdb.exc("db_msg_insert", [nzbname, "checking for sanity", "info"], {})
                sanity0 = self.do_sanity_check(self.allfileslist, files, infolist, bytescount0, self.filetypecounter)
                if sanity0 < 1:
                    if (self.filetypecounter["par2vol"]["max"] > 0 and sanity0 < self.crit_art_health_w_par) or\
                       (self.filetypecounter["par2vol"]["max"] == 0 and sanity0 < self.crit_art_health_wo_par):
                        self.pwdb.exc("db_msg_insert", [nzbname, "Sanity less than criticical health level, exiting", "error"], {})
                        self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                                  self.allfileslist)), "download failed", self.main_dir
                        sys.exit()

                    self.pwdb.exc("db_msg_insert", [nzbname, "Sanity less than 100%, preloading par2vols!", "warning"], {})
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    self.overall_size = self.overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set_sanity = ["par2vol"]
                    do_mpconnections(self.pipes, "pause", None)
                    self.clear_queues_and_pipes()
                    time.sleep(0.5)
                    do_mpconnections(self.pipes, "resume", None)
                else:
                    self.pwdb.exc("db_msg_insert", [nzbname, "Sanity is 100%, all OK!", "info"], {})

            # start decoder mpp
            self.logger.debug(whoami() + "starting decoder process ...")
            self.mpp_decoder = mp.Process(target=article_decoder.decode_articles, args=(self.mp_work_queue, self.mp_loggerqueue, self.filewrite_lock, ))
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
        do_mpconnections(self.pipes, "reset_timestamps", None)

        # download loop until articles downloaded
        oldrarcounter = 0
        self.pwdb.exc("db_msg_insert", [nzbname, "downloading", "info"], {})

        stopped_max_counter = 5
        getnextnzb = False
        article_failed = 0

        unrarer_idle_starttime = sys.maxsize
        last_rar_downloadedtime = sys.maxsize
        rarcounter = 0

        self.tt_wait_for_completion = None
        self.tt_wait_for_completion_unrar = None

        while self.stopped_counter < stopped_max_counter:

            # monitor idle times of unrarer to avoid deadlock further down
            if unrarer_idle_starttime == sys.maxsize and self.event_unrareridle.is_set():
                unrarer_idle_starttime = time.time()
            elif unrarer_idle_starttime != sys.maxsize and not self.event_unrareridle.is_set():
                unrarer_idle_starttime = sys.maxsize

            # if terminated: ensure that all tasks are processed
            if self.event_stopped.wait(0.25):
                self.stopped_counter += 1
                self.resume()
                self.logger.info(whoami() + "termination countdown: " + str(self.stopped_counter) + " of " + str(stopped_max_counter))
                time.sleep(0.25)

            return_reason = None

            if self.event_paused.isSet():
                continue

            self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
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

            # monitor unrarer
            if self.event_stopped.isSet() and mpp_is_alive(self.mpp, "unrarer"):
                self.logger.info(whoami() + "unrarer re-started, but should be dead, stopping ...")
                kill_mpp(self.mpp, "unrarer")
            if not self.event_stopped.isSet():
                unrarstatus = self.pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {})
                if unrarstatus == 1:    # if unrarer is busy, does it hang?
                    if not mpp_is_alive(self.mpp, "unrarer"):   # or\
                        # (time.time() - unrarer_idle_starttime > 5 and last_rar_downloadedtime >= unrarer_idle_starttime + 10):
                        self.logger.info(whoami() + "unrarer should run but is dead, restarting unrarer")
                        kill_mpp(self.mpp, "unrarer")
                        self.mpp_unrarer = mp.Process(target=partial_unrar.partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                                                nzbname, self.mp_loggerqueue, None, self.event_unrareridle,
                                                                                                self.cfg, ))
                        self.mpp_unrarer.start()
                        self.mpp["unrarer"] = self.mpp_unrarer
                elif unrarstatus == 2:  # finished but some artefacts remain?
                    if mpp_is_alive(self.mpp, "unrarer") or self.mpp["unrarer"]:
                        self.logger.debug(whoami() + "unrarer finished, but not cleaned up, cleaning ...")
                        kill_mpp(self.mpp, "unrarer")
                elif unrarstatus == -1:  # failed but still running
                    if mpp_is_alive(self.mpp, "unrarer") or self.mpp["unrarer"]:
                        self.logger.debug(whoami() + "unrarer finished, but not cleaned up, cleaning ...")
                        kill_mpp(self.mpp, "unrarer")
                elif unrarstatus == -2:  # failed bcause wrong first rar -> restart
                    self.logger.info(whoami() + "unrarer stopped due to wrong start rar, restarting ...")
                    kill_mpp(self.mpp, "unrarer")
                    self.mpp_unrarer = mp.Process(target=partial_unrar.partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                              nzbname, self.mp_loggerqueue, None, self.event_unrareridle, self.cfg, ))
                    self.mpp_unrarer.start()
                    self.mpp["unrarer"] = self.mpp_unrarer
                elif unrarstatus == 0:    # if unrarer should be state 0 but running -> kill!
                    if mpp_is_alive(self.mpp, "unrarer") or self.mpp["unrarer"]:
                        self.logger.debug(whoami() + "unrarer should be idle, but is not, cleaning ...")
                        kill_mpp(self.mpp, "unrarer")

            # monitor decoder
            if self.event_stopped.isSet() and mpp_is_alive(self.mpp, "decoder"):
                self.logger.info(whoami() + "decoder re-started, but should be dead, stopping ...")
                kill_mpp(self.mpp, "decoder")
            if not self.event_stopped.isSet() and self.mpp["decoder"] and not mpp_is_alive(self.mpp, "decoder"):
                self.logger.debug(whoami() + "restarting decoder process ...")
                self.mpp_decoder = mp.Process(target=article_decoder.decode_articles, args=(self.mp_work_queue, self.mp_loggerqueue, self.filewrite_lock, ))
                self.mpp_decoder.start()
                self.mpp["decoder"] = self.mpp_decoder

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
                    if filetype == "par2":
                        if not self.p2:
                            self.p2 = par2lib.Par2File(full_filename)
                            self.logger.info(whoami() + "found first par2 file")
                    if "par2" in inject_set0 and (filetype == "par2" or self.filetypecounter["par2"]["max"] == 0):
                        self.logger.debug(whoami() + "injecting rars etc.")
                        inject_set0 = ["etc", "nfo", "rar"]
                        files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                                             self.filetypecounter)
                        bytescount0 += bytescount00
                        article_count += article_count0
                except (queue.Empty, EOFError):
                    break
                except Exception:
                    break

            # if verifier tells us so, we load par2vol files, stop unrarer
            loadpar2vols0 = None
            if self.pipes["verifier"][0].poll():
                loadpar2vols0 = self.pipes["verifier"][0].recv()
            if not self.event_stopped.isSet() and not loadpar2vols and loadpar2vols0:
                loadpar2vols = True
                if self.filetypecounter["par2vol"]["max"] > 0:
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar(s) corrupt, loading par2vol files", "info"], {})
                    self.pwdb.exc("db_nzb_update_loadpar2vols", [nzbname, True], {})
                    self.overall_size = self.overall_size_wparvol
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                                         self.filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0
                else:
                    self.logger.error(whoami() + "rar files corrupt but cannot repair (no par2 files), exiting download")
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar files corrupt but cannot repair (no par2 files), exiting download", "error"], {})
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                    return_reason = "download failed"
                    self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                              self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                              self.allfileslist)), return_reason, self.main_dir
                    self.stop()
                    self.stopped_counter = stopped_max_counter
                    continue

            # monitor verifier
            if self.event_stopped.isSet() and mpp_is_alive(self.mpp, "verifier"):
                self.logger.info(whoami() + "verifier re-started, but should be dead, stopping ...")
                kill_mpp(self.mpp, "verifier")
            if not self.event_stopped.isSet() and mpp_is_alive(self.mpp, "verifier"):
                verifystatus = self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
                if verifystatus == 2:
                    self.mpp["verifier"].join()
                    self.mpp["verifier"] = None
                    self.logger.debug(whoami() + "verifier joined")
                # if unrarer is running and verifystatus is negative, stop!
                elif mpp_is_alive(self.mpp, "unrarer") and verifystatus == -2:
                    self.logger.debug(whoami() + "unrarer running but stopping/postponing now due to broken rar file!")
                    kill_mpp(self.mpp, "unrarer")
                    self.pwdb.exc("db_nzb_update_unrar_status", [nzbname, 0], {})
                    self.pwdb.exc("db_msg_insert", [nzbname, "par repair needed, postponing unrar", "info"], {})

            # check if unrarer should be started
            if not self.event_stopped.isSet() and unrarstatus == 0 and self.filetypecounter["rar"]["counter"] > oldrarcounter\
               and not self.pwdb.exc("db_nzb_get_ispw_checked", [nzbname], {}):
                # testing if pw protected
                rf = [rf0 for _, _, rf0 in os.walk(self.verifiedrar_dir) if rf0]
                # if no rar files in verified_rardir: skip as we cannot test for password
                if rf:
                    rarcounter += 1
                    last_rar_downloadedtime = time.time()
                    oldrarcounter = self.filetypecounter["rar"]["counter"]
                    self.logger.debug(whoami() + ": first/new verified rar file appeared, testing if pw protected")
                    is_pwp = passworded_rars.is_rar_password_protected(self.verifiedrar_dir, self.logger)
                    self.pwdb.exc("db_nzb_set_ispw_checked", [nzbname, True], {})
                    if is_pwp == 0:
                        self.logger.warning(whoami() + "cannot test rar if pw protected, something is wrong: " + str(is_pwp) + ", exiting ...")
                        self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})  # status download failed
                        return_reason = "download failed"
                        self.pwdb.exc("db_msg_insert", [nzbname, "download failed due to pw test not possible", "error"], {})
                        self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                                  self.allfileslist)), return_reason, self.main_dir
                        self.stop()
                        self.stopped_counter = stopped_max_counter
                        continue
                    elif is_pwp == -2:         # could not check -> check next time and continue downloading
                        self.logger.warning(whoami() + "cannot test rar if pw protected, but continuing ...")
                    if is_pwp == 1:
                        # if pw protected -> postpone password test + unrar
                        self.pwdb.exc("db_nzb_set_ispw", [nzbname, True], {})
                        self.pwdb.exc("db_msg_insert", [nzbname, "rar archive is password protected", "warning"], {})
                        self.logger.info(whoami() + "rar archive is pw protected, postponing unrar to postprocess ...")
                    elif is_pwp == -1:
                        # if not pw protected -> normal unrar
                        self.pwdb.exc("db_nzb_set_ispw", [nzbname, False], {})
                        verifystatus = self.pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
                        if verifystatus != -2:
                            self.logger.info(whoami() + "rar archive is not pw protected, starting unrarer ...")
                            kill_mpp(self.mpp, "unrarer")
                            self.pwdb.exc("db_nzb_update_unrar_status", [nzbname, 0], {})
                            self.logger.info(whoami() + "starting unrarer")
                            self.mpp_unrarer = mp.Process(target=partial_unrar.partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                                                    nzbname, self.mp_loggerqueue, None, self.event_unrareridle,
                                                                                                    self.cfg, ))
                            self.mpp_unrarer.start()
                            self.mpp["unrarer"] = self.mpp_unrarer
                        else:
                            if mpp_is_alive(self.mpp, "unrarer"):
                                kill_mpp(self.mpp, "unrarer")
                                self.pwdb.exc("db_nzb_update_unrar_status", [nzbname, 0], {})
                            self.logger.debug(whoami() + "not starting unrarer due to verifier state = -2")
                    elif is_pwp == -3:
                        self.logger.info(whoami() + ": cannot check for pw protection as first rar not present yet")
                else:
                    self.logger.debug(whoami() + "no rars in verified_rardir yet, cannot test for pw / start unrarer yet!")
            elif self.filetypecounter["rar"]["counter"] > rarcounter:
                self.logger.debug(whoami() + ": new rar downloaded")
                rarcounter += 1
                last_rar_downloadedtime = time.time()

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
                        self.mpp_verifier = mp.Process(target=par_verifier.par_verifier, args=(self.pipes["verifier"][1], self.rename_dir, self.verifiedrar_dir,
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
            if failed != 0:
                # stop if par2file cannot be downloaded
                par2failed = False
                for fname, item in files.items():
                    f_nr_articles, f_age, f_filetype, _, failed0 = item
                    if (failed0 and f_filetype == "par2"):
                        par2failed = True
                        break
                # stop if rar failed and no parvol files
                rarfailed_wo_parvol = False
                if self.filetypecounter["par2vol"]["max"] == 0:
                    for fname, item in files.items():
                        f_nr_articles, f_age, f_filetype, _, failed0 = item
                        if (failed0 and f_filetype == "rar"):
                            rarfailed_wo_parvol = True
                            break

                self.logger.warning(whoami() + str(failed) + " articles failed, article_count: " + str(article_count) + ", health: " + str(article_health)
                                    + ", par2failed: " + str(par2failed) + ", rarfailed_wo_parvol: " + str(rarfailed_wo_parvol))
                # if too many missing articles: exit download
                if rarfailed_wo_parvol or par2failed or article_health <= self.crit_art_health:
                    self.logger.info(whoami() + "articles missing and cannot repair, exiting download")
                    self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                    if par2failed:
                        self.pwdb.exc("db_msg_insert", [nzbname, "par2 file broken/not available on servers", "error"], {})
                    elif rarfailed_wo_parvol:
                        self.pwdb.exc("db_msg_insert", [nzbname, "rar file broken and repair files available", "error"], {})
                    else:
                        self.pwdb.exc("db_msg_insert", [nzbname, "critical health threashold exceeded", "error"], {})
                    return_reason = "download failed"
                    self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                              self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                              self.allfileslist)), return_reason, self.main_dir
                    self.stop()
                    self.stopped_counter = stopped_max_counter
                    continue
                if not loadpar2vols and self.filetypecounter["par2vol"]["max"] > 0 and article_health > self.crit_art_health:
                    self.logger.info(whoami() + "queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, self.allfileslist, files, infolist, bytescount0,
                                                                                         self.filetypecounter)
                    bytescount0 += bytescount00
                    article_count += article_count0
                    self.overall_size = self.overall_size_wparvol
                    loadpar2vols = True
                    self.pwdb.exc("db_msg_insert", [nzbname, "rar(s) corrupt, loading par2vol files", "info"], {})

            # check if all files are downloaded
            getnextnzb = True
            for filetype, item in self.filetypecounter.items():
                if filetype == "par2vol" and not loadpar2vols:
                    continue
                if filetype == "par2" and (self.filetypecounter["par2"]["counter"] < self.filetypecounter["par2"]["max"]):
                    par2l = filter(lambda parl0: parl0.endswith("_sample.par2"), self.filetypecounter[filetype]["filelist"])
                    if par2l:
                        continue
                if self.filetypecounter[filetype]["counter"] < self.filetypecounter[filetype]["max"]:
                    getnextnzb = False
                    break

            # check not getnextnzb & speed = 0 -> stuck, downloadstatus = -2
            dl_stuck = False
            if not getnextnzb and self.all_queues_are_empty():
                if not self.tt_wait_for_completion:
                    self.tt_wait_for_completion = time.time()
                else:
                    if time.time() - self.tt_wait_for_completion > self.connection_idle_timeout:
                        dl_stuck = True
            elif not getnextnzb:
                self.tt_wait_for_completion = None

            # all downloaded but unrarer still running(stuck?)
            unrarer_is_stuck = False
            if self.event_unrareridle.is_set() and (getnextnzb or self.all_queues_are_empty()):
                if not self.tt_wait_for_completion_unrar:
                    self.tt_wait_for_completion_unrar = time.time()
                    self.pwdb.exc("db_msg_insert", [nzbname, "waiting for unrarer to resume/finish", "info"], {})
                else:
                    if time.time() - self.tt_wait_for_completion_unrar > self.connection_idle_timeout:
                        unrarer_is_stuck = True
            elif not getnextnzb or not self.event_unrareridle.is_set():
                self.tt_wait_for_completion_unrar = None

            # if unrarer stuck or all files are downloaded and still articles in queue --> inconsistency, exit!
            if unrarer_is_stuck or dl_stuck or (getnextnzb and not self.all_queues_are_empty()):
                if unrarer_is_stuck:
                    self.pwdb.exc("db_msg_insert", [nzbname, "unrarer is stuck!", "error"], {})
                elif dl_stuck:
                    self.pwdb.exc("db_msg_insert", [nzbname, "download is stuck!", "error"], {})
                else:
                    self.pwdb.exc("db_msg_insert", [nzbname, "inconsistency in download queue", "error"], {})
                self.pwdb.exc("db_nzb_update_status", [nzbname, -2], {})
                if unrarer_is_stuck:
                    self.logger.error(whoami() + ": all articles/files downloaded but unrarer still waiting, exiting ...")
                elif dl_stuck:
                    self.logger.error(whoami() + ": all queues are empty but not completed, exiting ...")
                else:
                    self.logger.error(whoami() + ": records say dl is done, but still some articles in queue, exiting ...")
                return_reason = "download failed"
                self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                          self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                          self.allfileslist)), return_reason, self.main_dir
                self.stop()
                self.stopped_counter = stopped_max_counter
                continue

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
        elif return_reason.endswith("failed") or return_reason == "download success, but no postproc. possible!":
            msgtype = "error"
        elif return_reason.endswith("success"):
            msgtype = "success"
        else:
            msgtype = "info"
        # self.pwdb.exc("db_msg_insert", [nzbname, return_reason, msgtype], {})
        self.results = nzbname, ((bytescount0, self.allbytesdownloaded0, availmem0, avgmiblist, self.filetypecounter, nzbname, article_health,
                                  self.overall_size, self.already_downloaded_size, self.p2, self.overall_size_wparvol,
                                  self.allfileslist)), return_reason, self.main_dir

    def get_level_servers(self, retention):
        result = do_mpconnections(self.pipes, "get_level_servers", retention)
        return result
