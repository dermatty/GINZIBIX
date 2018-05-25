#!/home/stephan/.virtualenvs/nntp/bin/python

import time
import sys
import os
import queue
from os.path import expanduser
import signal
import inotify_simple
import multiprocessing as mp
import logging
import logging.handlers
import psutil
import re
import threading
import datetime
import shutil
import glob
from .renamer import renamer
from .par_verifier import par_verifier
from .par2lib import Par2File
from .partial_unrar import partial_unrar
from .nzb_parser import ParseNZB
from .article_decoder import decode_articles
from .passworded_rars import is_rar_password_protected, get_password
from .connections import ConnectionWorker
from .server import Servers

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
subdirs = {
    "download": "_downloaded0",
    "renamed": "_renamed0",
    "unpacked": "_unpack0",
    "verififiedrar": "_verifiedrars0"
}
_ftypes = ["etc", "rar", "sfv", "par2", "par2vol"]


class SigHandler_Main:

    def __init__(self, mpp, ct, mp_work_queue, logger):
        self.logger = logger
        self.servers = ct.servers
        self.threads = ct.threads
        self.mpp = mpp
        self.mp_work_queue = mp_work_queue

    def shutdown(self):
        f = open('/dev/null', 'w')
        sys.stdout = f
        for key, item in self.mpp.items():
            if item:
                item_pid = str(item.pid)
            else:
                item_pid = "-"
            self.logger.debug("MPP " + key + ", pid = " + item_pid)
        # stop unrarer
        mpid = None
        if self.mpp["unrarer"]:
            mpid = self.mpp["unrarer"].pid
        if mpid:
            # if self.mpp["unrarer"].pid:
            self.logger.warning("signalhandler: terminating unrarer")
            try:
                os.kill(self.mpp["unrarer"].pid, signal.SIGKILL)
                self.mpp["unrarer"].join()
            except Exception as e:
                self.logger.debug(str(e))
        # stop rar_verifier
        mpid = None
        if self.mpp["verifier"]:
            mpid = self.mpp["verifier"].pid
        if mpid:
            self.logger.warning("signalhandler: terminating rar_verifier")
            try:
                os.kill(self.mpp["verifier"].pid, signal.SIGKILL)
                self.mpp["verifier"].join()
            except Exception as e:
                self.logger.debug(str(e))
        # stop mpp_renamer
        mpid = None
        if self.mpp["renamer"]:
            mpid = self.mpp["renamer"].pid
        if mpid:
            self.logger.warning("signalhandler: terminating renamer")
            try:
                os.kill(self.mpp["renamer"].pid, signal.SIGKILL)
                self.mpp["renamer"].join()
            except Exception as e:
                self.logger.debug(str(e))
        # stop nzbparser
        mpid = None
        if self.mpp["nzbparser"]:
            mpid = self.mpp["nzbparser"].pid
        if mpid:
            self.logger.warning("signalhandler: terminating nzb_parser")
            try:
                os.kill(self.mpp["nzbparser"].pid, signal.SIGKILL)
                self.mpp["nzbparser"].join()
            except Exception as e:
                self.logger.debug(str(e))
        # stop article decoder
        mpid = None
        if self.mpp["decoder"]:
            mpid = self.mpp["decoder"].pid
        if mpid:
            self.logger.warning("signalhandler: terminating article_decoder")
            self.mp_work_queue.put(None)
            time.sleep(1)
            try:
                os.kill(self.mpp["decoder"].pid, signal.SIGKILL)
                self.mpp["decoder"].join()
            except Exception as e:
                self.logger.debug(str(e))
        # threads + servers
        self.logger.warning("signalhandler: stopping download threads")
        for t, _ in self.threads:
            t.stop()
            t.join()
        if self.servers:
            self.logger.warning("signalhandler: closing all server connections")
            self.servers.close_all_connections()
        self.logger.warning("signalhandler: exiting")
        sys.exit()

    def sighandler(self, a, b):
        self.shutdown()
        

# logginghandler for storing msgs in db
class NZBHandler(logging.Handler):
    def __init__(self, arg0, pwdb):
        logging.Handler.__init__(self)
        self.nzbname = arg0
        self.pwdb = pwdb

    def emit(self, record):
        # record.message is the log message
        self.pwdb.db_msg_insert(self.nzbname, record.message, record.levelname)


# Handles download of a NZB file
class Downloader():
    def __init__(self, cfg, dirs, pwdb, ct, mp_work_queue, sighandler, mpp, logger):
        self.cfg = cfg
        self.pwdb = pwdb
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
        try:
            self.pw_file = self.dirs["main"] + self.cfg["FOLDERS"]["PW_FILE"]
            self.logger.debug("Password file is: " + self.pw_file)
        except Exception as e:
            self.logger.warning(str(e) + ": no pw file provided!")
            self.pw_file = None
        if self.pw_file:
            try:
                self.logger.debug("As a first test, open pw_file")
                with open(self.pw_file, "r") as f0:
                    f0.readlines()
                self.logger.debug("pw_file ok")
            except Exception as e:
                self.logger.warning(str(e) + ": cannot open pw file, setting to None")
                self.pw_file = None

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
            self.logger.error(str(e) + " in creating dirs ...")
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

    def article_producer(self, articles, articlequeue):
        for article in articles:
            articlequeue.put(article)

    def display_console_connection_data(self, bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health,
                                        overall_size, already_downloaded_size):
        avgmiblist = avgmiblist00
        max_mem_needed = 0
        bytescount0 = bytescount00
        bytescount0 += 0.00001
        overall_size += 0.00001
        availmem0 = availmem00
        # get Mib downloaded
        if len(avgmiblist) > 50:
            del avgmiblist[0]
        if len(avgmiblist) > 10:
            avgmib_dic = {}
            for (server_name, _, _, _, _, _, _, _, _) in self.ct.servers.server_config:
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
        if nzbname:
            print("--> Downloading " + nzbname)
        else:
            print("--> Downloading paused!" + " " * 30)
        try:
            print("MBit/sec.: " + str([sn + ": " + str(int(av)) + "  " for sn, av in avgmib_dic.items()]) + " max. mem_needed: "
                  + "{0:.3f}".format(max_mem_needed) + " GB                 ")
        except UnboundLocalError:
            print("MBit/sec.: --- max. mem_needed: " + str(max_mem_needed) + " GB                ")
        gbdown0 = 0
        mbitsec0 = 0
        for k, (t, last_timestamp) in enumerate(self.ct.threads):
            gbdown = t.bytesdownloaded / (1024 * 1024 * 1024)
            gbdown0 += gbdown
            gbdown_str = "{0:.3f}".format(gbdown)
            mbitsec = (t.bytesdownloaded / (time.time() - t.last_timestamp)) / (1024 * 1024) * 8
            mbitsec0 += mbitsec
            mbitsec_str = "{0:.1f}".format(mbitsec)
            print(t.idn + ": Total - " + gbdown_str + " GB" + " | MBit/sec. - " + mbitsec_str + "                        ")
        print("-" * 60)
        gbdown0 += already_downloaded_size
        gbdown0_str = "{0:.3f}".format(gbdown0)
        perc0 = gbdown0 / overall_size
        if perc0 > 1:
            perc00 = 1
        else:
            perc00 = perc0
        print(gbdown0_str + " GiB (" + "{0:.1f}".format(perc00 * 100) + "%) of total "
              + "{0:.2f}".format(overall_size) + " GiB | MBit/sec. - " + "{0:.1f}".format(mbitsec0) + " " * 10)
        for key, item in filetypecounter00.items():
            print(key + ": " + str(item["counter"]) + "/" + str(item["max"]) + ", ", end="")
        if nzbname:
            trailing_spaces = " " * 10
        else:
            trailing_spaces = " " * 70
        print("Health: {0:.4f}".format(article_health * 100) + "%" + trailing_spaces)
        if mbitsec0 > 0 and perc0 < 1:
            eta = ((overall_size - gbdown0) * 1024)/(mbitsec0 / 8)
            print("Eta: " + str(datetime.timedelta(seconds=int(eta))) + " " * 30)
        else:
            print("Eta: - (done!)" + " " * 40)
        print()
        msg0, ll = self.pwdb.db_msg_get(nzbname)
        if msg0:
            print(msg0[ll].message[:60])
        for _ in range(len(self.ct.threads) + 8):
            sys.stdout.write("\033[F")

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
        for j, file_articles in enumerate(reversed(filelist)):
            # iterate over all articles in file
            filename, age, filetype, nr_articles = file_articles[0]
            # check if file already exists in "incomplete"
            if filetype in ftypes:
                level_servers = self.get_level_servers(age)
                files[filename] = (nr_articles, age, filetype, False, True)
                infolist[filename] = [None] * nr_articles
                self.pwdb.db_file_update_status(filename, 1)   # status do downloading
                for i, art0 in enumerate(file_articles):
                    if i == 0:
                        continue
                    art_nr, art_name, art_bytescount = art0
                    bytescount0 += art_bytescount
                    q = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
                    self.articlequeue.put(q)
                    article_count += 1
        bytescount0 = bytescount0 / (1024 * 1024 * 1024)
        self.logger.debug("Art. Count: " + str(article_count))
        return files, infolist, bytescount0, article_count

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
                filename, age, filetype, nr_articles, art_nr, art_name, download_server, inf0 = resultarticle
                bytesdownloaded = sum(len(i) for i in inf0)
                if inf0 == "failed":
                    failed += 1
                    inf0 = empty_yenc_article
                    self.logger.error(filename + "/" + art_name + ": failed!!")
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
                    self.logger.info("All articles for " + filename + " downloaded, calling mp.decode ...")
            except KeyError:
                pass
            except queue.Empty:
                break
        return newresult, avgmiblist, infolist, files, failed

    # postprocessor
    def postprocess_nzb(self, nzbname, downloaddata):
        # self.sighandler.shutdown()
        bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health, overall_size, already_downloaded_size = downloaddata
        self.display_console_connection_data(bytescount0, availmem0, avgmiblist, filetypecounter, nzbname,
                                             article_health, overall_size, already_downloaded_size)
        self.resultqueue.join()
        self.articlequeue.join()
        # join renamer
        if self.mpp["renamer"]:
            try:
                # to do: loop over downloaded and wait until empty
                self.logger.debug("Waiting for renamer.py clearing download dir")
                while True:
                    for _, _, fs in os.walk(self.download_dir):
                        if not fs:
                            break
                    else:
                        time.sleep(1)
                        continue
                    break
                self.logger.debug("Download dir empty!")
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
                    except queue.Empty:
                        break
                self.mpp["verifier"].join()
            except Exception as e:
                self.logger.warning(str(e))
            self.mpp["verifier"] = None
        # if pw protected -> get pw and start unrarer
        ispw = self.pwdb.db_nzb_get_ispw(nzbname)
        if ispw:
            if self.pwdb.db_nzb_get_password(nzbname) == "N/A":
                self.logger.info("Trying to get password from file for NZB " + nzbname)
                pw = get_password(self.verifiedrar_dir, self.pw_file, nzbname, self.logger)
                if pw:
                    self.logger.info("Found password " + pw + " for NZB " + nzbname)
                    self.pwdb.db_nzb_set_password(nzbname, pw)
            else:
                pw = self.pwdb.db_nzb_get_password(nzbname)
            if not pw:
                self.logger.error("Cannot find password for NZB " + nzbname + "in postprocess, exiting ...")
                self.pwdb.db_nzb_update_status(nzbname, -4)
                return -1
            self.mpp_unrarer = mp.Process(target=partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                      self.pwdb, nzbname, self.logger, pw, ))
            self.mpp_unrarer.start()
            self.mpp["unrarer"] = self.mpp_unrarer
            self.sighandler.mpp = self.mpp
        finalverifierstate = (self.pwdb.db_nzb_get_verifystatus(nzbname) in [0, 2])
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
                    self.logger.debug(str(e))
            self.mpp["unrarer"] = None
            self.sighandler.mpp = self.mpp
            self.logger.debug("Unrarer stopped!")
        # get status
        finalverifierstate = (self.pwdb.db_nzb_get_verifystatus(nzbname) in [0, 2])
        finalnonrarstate = self.pwdb.db_allnonrarfiles_getstate(nzbname)
        finalrarstate = (self.pwdb.db_nzb_get_unrarstatus(nzbname) in [0, 2])
        self.logger.info("Finalrarstate: " + str(finalrarstate) + " / Finalnonrarstate: " + str(finalnonrarstate))
        if finalrarstate and finalnonrarstate and finalverifierstate:
            self.logger.info("Postprocess of NZB " + nzbname + " ok!")
        else:
            self.pwdb.db_nzb_update_status(nzbname, -4)
            self.logger.info("Postprocess of NZB " + nzbname + " failed!")
            return -1
        # copy to complete
        res0 = self.make_complete_dir()
        if not res0:
            self.pwdb.db_nzb_update_status(nzbname, -4)
            self.logger.info("Cannot create complete_dir for " + nzbname + ", exiting ...")
            return -1
        # move all non-rar/par2/par2vol files from renamed to complete
        for f00 in glob.glob(self.rename_dir + "*"):
            self.logger.debug("Renamed_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
            if os.path.isdir(f00):
                self.logger.debug(f00 + "is a directory, skipping")
                continue
            f0 = f00.split("/")[-1]
            file0 = self.pwdb.db_file_get_renamed(f0)
            self.logger.debug("Moving/deleting " + f0)
            if not file0:
                gg = re.search(r"[0-9]+[.]rar[.]+[0-9]", f0, flags=re.IGNORECASE)
                if gg:
                    try:
                        os.remove(f00)
                        self.logger.debug("Removed rar.x file " + f0)
                    except Exception as e:
                        self.pwdb.db_nzb_update_status(nzbname, -4)
                        self.logger.warning(str(e) + ": cannot remove corrupt rar file!")
                else:    # if unknown file (not in db) move to complete anyway
                    try:
                        shutil.move(f00, self.complete_dir)
                        self.logger.debug("Moved " + f00 + " to " + self.complete_dir)
                    except Exception as e:
                        self.logger.warning(str(e) + ": cannot move unknown file to complete!")
                continue
            if file0.ftype in ["rar", "par2", "par2vol"]:
                try:
                    os.remove(f00)
                    self.logger.debug("Removed rar/par2 file " + f0)
                except Exception as e:
                    self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.logger.warning(str(e) + ": cannot remove rar/par2 file!")
            else:
                try:
                    shutil.move(f00, self.complete_dir)
                    self.logger.debug("Moved non-rar/non-par2 file " + f0 + " to complete")
                except Exception as e:
                    self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.logger.warning(str(e) + ": cannot move non-rar/non-par2 file " + f00 + "!")
        # remove download_dir
        try:
            shutil.rmtree(self.download_dir)
        except Exception as e:
            self.pwdb.db_nzb_update_status(nzbname, -4)
            self.logger.warning(str(e) + ": cannot remove download_dir!")
        # move content of unpack dir to complete
        self.logger.debug("Moving unpack_dir: " + self.unpack_dir)
        for f00 in glob.glob(self.unpack_dir + "*"):
            self.logger.debug("Unpack_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
            if not os.path.isdir(f00):
                try:
                    shutil.move(f00, self.complete_dir)
                except Exception as e:
                    self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.logger.warning(str(e) + ": cannot move unrared file to complete dir!")
            else:
                d0 = f00.split("/")[-1]
                if os.path.isdir(self.complete_dir + d0):
                    try:
                        shutil.rmtree(self.complete_dir + d0)
                    except Exception as e:
                        self.pwdb.db_nzb_update_status(nzbname, -4)
                        self.logger.warning(str(e) + ": cannot remove unrared dir in complete!")
                try:
                    shutil.copytree(f00, self.complete_dir + d0)
                except Exception as e:
                    self.pwdb.db_nzb_update_status(nzbname, -4)
                    self.logger.warning(str(e) + ": cannot move non-rar/non-par2 file!")
        # remove unpack_dir
        if self.pwdb.db_nzb_getstatus(nzbname) != -4:
            try:
                shutil.rmtree(self.unpack_dir)
                shutil.rmtree(self.verifiedrar_dir)
            except Exception as e:
                self.pwdb.db_nzb_update_status(nzbname, -4)
                self.logger.warning(str(e) + ": cannot remove unpack_dir / verifiedrar_dir")
        # remove incomplete_dir
        if self.pwdb.db_nzb_getstatus(nzbname) != -4:
            try:
                shutil.rmtree(self.main_dir)
            except Exception as e:
                self.pwdb.db_nzb_update_status(nzbname, -4)
                self.logger.warning(str(e) + ": cannot remove incomplete_dir!")
        # finalize
        if self.pwdb.db_nzb_getstatus(nzbname) == -4:
            self.logger.info("Copy/Move of NZB " + nzbname + " failed!")
            return -1
        else:
            self.logger.info("Copy/Move of NZB " + nzbname + " success!")
            self.pwdb.db_nzb_update_status(nzbname, 4)
            return 1

    # main download routine
    def download(self, allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2):
        # directories
        #    incomplete/_downloaded0:   all files go straight to here after download
        #    incomplete/_verifiedrar0:  downloaded & verfied rars, ready to unrar
        #    incomplete/_unpack0:       unpacked verified rars
        #    incomplete:                final directory before moving to complete

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
        if filetypecounter["par2"]["max"] > 0 and filetypecounter["par2"]["max"] > filetypecounter["par2"]["counter"]:
            inject_set0 = ["par2"]
        elif self.pwdb.db_nzb_loadpar2vols(nzbname):
            inject_set0 = ["par2vol", "rar", "sfv", "nfo", "etc"]
            loadpar2vols = True
        else:
            inject_set0 = ["rar", "sfv", "nfo", "etc"]
        self.logger.info("Overall_Size: " + str(overall_size) + ", incl. par2vols: " + str(overall_size_wparvol))
        # make dirs
        self.logger.debug("Making dirs for NZB: " + str(nzbname))
        self.make_dirs(nzbname)
        # start renamer
        self.logger.debug("Starting renamer process for NZB " + nzbname)
        self.mpp_renamer = mp.Process(target=renamer, args=(self.download_dir, self.rename_dir, self.pwdb, self.mp_result_queue, self.logger, ))
        self.mpp_renamer.start()
        self.mpp["renamer"] = self.mpp_renamer
        self.sighandler.mpp = self.mpp
        # start download threads
        if not self.ct.threads:
            self.ct.start_threads()
            self.sighandler.servers = self.ct.servers
        else:
            self.ct.reset_timestamps()
        self.logger.info("Downloading articles for: " + self.nzb)
        self.pwdb.db_nzb_update_status(nzbname, 2)    # status "downloading"
        bytescount0 = self.getbytescount(allfileslist)
        files, infolist, bytescount0, article_count = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
        getnextnzb = False
        article_failed = 0

        while True:

            # get mp_result_queue (from article_decoder.py)
            while True:
                try:
                    filename, full_filename, filetype, old_filename, old_filetype = self.mp_result_queue.get_nowait()
                    if old_filename != filename or old_filetype != filetype:
                        self.logger.debug(old_filename + "/" + old_filetype + " changed to " + filename + " / " + filetype)
                        # update filetypecounter
                        filetypecounter[old_filetype]["filelist"].remove(old_filename)
                        filetypecounter[filetype]["filelist"].append(filename)
                        filetypecounter[old_filetype]["max"] -= 1
                        filetypecounter[filetype]["counter"] += 1
                        filetypecounter[filetype]["loadedfiles"].append((filename, full_filename))
                        # update allfileslist
                        for i, o_lists in enumerate(allfileslist):
                            o_orig_name, o_age, o_type, o_nr_articles = o_lists[0]
                            if o_orig_name == old_filename:
                                allfileslist[i][0] = (filename, o_age, o_type, o_nr_articles)
                    else:
                        filetypecounter[filetype]["counter"] += 1
                        filetypecounter[filetype]["loadedfiles"].append((filename, full_filename))
                    if (filetype == "par2" or filetype == "par2vol") and not p2:
                        p2 = Par2File(full_filename)
                        self.logger.info("Sending " + filename + "-p2 object to parverify_queue")
                        # self.mp_parverify_outqueue.put(p2)
                    if inject_set0 == ["par2"] and (filetype == "par2" or filetypecounter["par2"]["max"] == 0):
                        inject_set0 = ["rar", "sfv", "nfo", "etc"]
                        files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                        bytescount0 += bytescount00
                        article_count += article_count0
                except queue.Empty:
                    break

            # get mp_parverify_inqueue
            if not loadpar2vols:
                while True:
                    try:
                        loadpar2vols = self.mp_parverify_inqueue.get_nowait()
                    except queue.Empty:
                        break
                if loadpar2vols:
                    self.pwdb.db_nzb_update_loadpar2vols(nzbname, True)
                    overall_size = overall_size_wparvol
                    self.logger.info("Queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                    bytescount0 += bytescount00
                    article_count += article_count0

            if filetypecounter["rar"]["counter"] >= 1 and not self.pwdb.db_nzb_get_ispw(nzbname) and not self.mpp["unrarer"]:
                # testing if pw protected
                rf = [rf0 for _, _, rf0 in os.walk(self.verifiedrar_dir) if rf0]
                # if no rar files in verified_rardir: skip as we cannot test for password
                if rf:
                    self.logger.info("First verified rar file appeared, testing if pw protected")
                    is_pwp = is_rar_password_protected(self.verifiedrar_dir, self.logger)
                    if is_pwp in [0, -2]:
                        self.logger.info("Cannot test rar if pw protected, something is wrong: " + str(is_pwp) + ", exiting ...")
                        self.pwdb.db_nzb_update_status(nzbname, -2)  # status download failed
                        getnextnzb = True
                        continue
                    if is_pwp == 1:
                        # if pw protected -> postpone password test + unrar
                        self.pwdb.db_nzb_set_ispw(nzbname, True)
                        self.logger.info("rar archive is pw protected, postponing unrar to postprocess ...")
                    else:
                        # if not pw protected -> normal unrar
                        self.logger.info("rar archive is not pw protected, starting unrarer ...")
                        self.pwdb.db_nzb_set_ispw(nzbname, False)
                        self.mpp_unrarer = mp.Process(target=partial_unrar, args=(self.verifiedrar_dir, self.unpack_dir,
                                                                                  self.pwdb, nzbname, self.logger, None, ))
                        self.mpp_unrarer.start()
                        self.mpp["unrarer"] = self.mpp_unrarer
                        self.sighandler.mpp = self.mpp
                else:
                    self.logger.debug("No rars in verified_rardir yet, cannot test for pw / start unrarer yet!")
            # if par2 available start par2verifier, else just copy rars unchecked!
            if not self.mpp["verifier"]:
                pvmode = None
                if p2:
                    pvmode = "verify"
                elif not p2 and filetypecounter["par2"]["max"] == 0:
                    pvmode = "copy"
                if pvmode:
                    self.logger.debug("Starting rar_verifier process (mode=" + pvmode + ")for NZB " + nzbname)
                    self.mpp_verifier = mp.Process(target=par_verifier, args=(self.mp_parverify_inqueue, self.rename_dir, self.verifiedrar_dir,
                                                                              self.main_dir, self.logger, self.pwdb, nzbname, pvmode, ))
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
            if failed != 0:
                self.logger.warning(str(failed) + " articles failed, article_count: " + str(article_count) + ", health: " + str(article_health))
                if not loadpar2vols:
                    self.logger.info("Queuing par2vols")
                    inject_set0 = ["par2vol"]
                    files, infolist, bytescount00, article_count0 = self.inject_articles(inject_set0, allfileslist, files, infolist, bytescount0)
                    bytescount0 += bytescount00
                    article_count += article_count0
                    overall_size = overall_size_wparvol
                    loadpar2vols = True

            self.display_console_connection_data(bytescount0, availmem0, avgmiblist, filetypecounter, nzbname,
                                                 article_health, overall_size, already_downloaded_size)

            # if all downloaded postprocess
            getnextnzb = True
            for filetype, item in filetypecounter.items():
                if filetype == "par2vol" and not loadpar2vols:
                    continue
                if filetypecounter[filetype]["counter"] < filetypecounter[filetype]["max"]:
                    getnextnzb = False
                    break
            if getnextnzb:
                self.logger.debug(nzbname + "- download complete!")
                self.pwdb.db_nzb_update_status(nzbname, 3)
                break

            time.sleep(0.2)

        return nzbname, ((bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,
                          overall_size, already_downloaded_size))

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
    # wait for new nzbs to arrive
    logger.debug("looking for new NZBs ...")
    try:
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2 \
            = make_allfilelist_inotify(pwdb, dirs, logger, -1)
    except Exception as e:
        logger.warning("!!! " + str(e))
    # poll for 30 sec if no nzb immediately found
    if not nzbname:
        logger.debug("polling for 30 sec. for new NZB before closing connections if alive ...")
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2 \
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
            logger.debug("Polling for new NZBs now in blocking mode!")
            try:
                allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2 \
                    = make_allfilelist_inotify(pwdb, dirs, logger, None)
            except Exception as e:
                logger.warning("!!! " + str(e))
    return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2


def make_allfilelist_inotify(pwdb, dirs, logger, timeout0):
    # immediatley get allfileslist
    if timeout0 and timeout0 <= -1:
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2 \
            = pwdb.make_allfilelist(dirs["incomplete"], dirs["nzb"])
        if nzbname:
            logger.debug("Inotify: no timeout, got nzb " + nzbname + " immediately!")
            return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2
        else:
            return None, None, None, None, None, None, None
    # setup inotify
    logger.debug("Setting up inotify for timeout=" + str(timeout0))
    pwdb_inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    wd = pwdb_inotify.add_watch(dirs["main"], watch_flags)
    t0 = time.time()
    timeout00 = timeout0
    while True:
        for event in pwdb_inotify.read(timeout=timeout00):
            logger.debug("got notify event on " + str(event.name))
            if event.name == u"ginzibix.db":
                logger.debug("Database updated, now checking for nzbname & data")
                allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2 \
                    = pwdb.make_allfilelist(dirs["incomplete"], dirs["nzb"])
                if nzbname:
                    logger.debug("new nzb found in db, queuing ...")
                    pwdb_inotify.rm_watch(wd)
                    return allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2
                else:
                    logger.debug("no new nzb found in db, continuing polling ...")
        # if timeout == None: again blocking, else subtract already spent timeout
        if timeout00:
            timeout00 = timeout00 - (time.time() - t0) * 1000
            t0 = time.time()
            if timeout00 <= 0:
                pwdb_inotify.rm_watch(wd)
                return None, None, None, None, None, None, None


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
            self.logger.debug("Starting download threads")
            self.init_servers()
            for sn, scon, _, _ in self.all_connections:
                t = ConnectionWorker(self.lock, (sn, scon), self.articlequeue, self.resultqueue, self.servers, self.logger)
                self.threads.append((t, time.time()))
                t.start()
        else:
            self.logger.debug("Threads already started")

    def reset_timestamps(self):
        for t, _ in self.threads:
            t.last_timestamp = time.time()

    def reset_timestamps_bdl(self):
        if self.threads:
            for t, _ in self.threads:
                t.bytesdownloaded = 0
                t.last_timestamp = 0


def ginzi_main(cfg, pwdb, logger):

    articlequeue = queue.LifoQueue()
    resultqueue = queue.Queue()
    mp_work_queue = mp.Queue()
    ct = ConnectionThreads(cfg, articlequeue, resultqueue, logger)

    # init sighandler
    mpp = {"nzbparser": None, "decoder": None, "unrarer": None, "renamer": None, "verifier": None}
    sh = SigHandler_Main(mpp, ct, mp_work_queue, logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    # start nzb parser mpp
    logger.debug("Starting nzbparser process ...")
    mpp_nzbparser = mp.Process(target=ParseNZB, args=(pwdb, dirs["nzb"], logger, ))
    mpp_nzbparser.start()
    mpp["nzbparser"] = mpp_nzbparser

    # start decoder mpp
    logger.debug("Starting decoder process ...")
    mpp_decoder = mp.Process(target=decode_articles, args=(mp_work_queue, pwdb, logger, ))
    mpp_decoder.start()
    mpp["decoder"] = mpp_decoder

    sh.mpp = mpp

    logging.handlers.NZBHandler = NZBHandler

    dl = Downloader(cfg, dirs, pwdb, ct, mp_work_queue, sh, mpp, logger)
    while True:
        allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2 \
            = get_next_nzb(pwdb, dirs, ct, logger)
        ct.reset_timestamps_bdl()

        # setup db logging handler for NZB
        nzbhandler = logging.handlers.NZBHandler(nzbname, pwdb)
        logger.addHandler(nzbhandler)
        nzbhandler.setLevel(logging.INFO)

        # download nzb
        nzbname, downloaddata = dl.download(allfileslist, filetypecounter, nzbname, overall_size, overall_size_wparvol, already_downloaded_size, p2)

        stat0 = pwdb.db_nzb_getstatus(nzbname)
        # if download success, postprocess
        if stat0 == 3:    # if download ok - postprocess
            dl.postprocess_nzb(nzbname, downloaddata)

        # delete NZB / db logging handler
        pwdb.db_msg_removeall(nzbname)
        logger.removeHandler(nzbhandler)
        del nzbhandler

