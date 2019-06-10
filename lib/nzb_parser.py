import os
import sys
import glob
import xml.etree.ElementTree as ET
import time
from .par2lib import get_file_type
from .aux import PWDBSender
from .mplogging import setup_logger, whoami
import inotify_simple
from lxml import etree
import signal

import re
from setproctitle import setproctitle


TERMINATED = False


class SigHandler_Parser:
    def __init__(self, logger):
        self.logger = logger

    def sighandler(self, a, b):
        global TERMINATED
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


def decompose_nzb(nzb, logger):
    maxtry = 10
    i = 0
    while True:
        try:
            tree = ET.parse(nzb)
            logger.info(whoami() + "loaded NZB file " + nzb)
            break
        except Exception as e:
            logger.warning(whoami() + str(e) + ": cannot load NZB file " + nzb + ", trying lxml module in 1 sec.")
            try:
                time.sleep(1)
                tree = etree.parse(nzb)
                logger.info(whoami() + "loaded NZB file " + nzb)
                break
            except Exception as e:
                i += 1
                if i >= maxtry:
                    logger.error(whoami() + str(e) + ": cannot load NZB file " + nzb + ", aborting ...")
                    return None, None
                else:
                    logger.warning(whoami() + str(e) + ": cannot load NZB file " + nzb + " with lxml module, retrying in 1 sec.")
        time.sleep(1)
    nzbroot = tree.getroot()
    filedic = {}
    bytescount0 = 0
    hnlist = []
    for r in nzbroot:
        headers = r.attrib
        try:
            date = headers["date"]
            age = int((time.time() - float(date))/(24 * 3600))
            subject = headers["subject"]
            # 1. Rule: search for quotation marks
            try:
                matches = re.findall(r'\"(.+?)\"', subject)
            except Exception:
                matches = None
            if matches and len(matches) == 1:
                hn = matches[0]
            else:
                # 2nd Rule: split yenc[0]
                subjectyenc = subject.split("yEnc")
                # if we cannot split by "yenc" -> take the whole subject name and hope for the best
                if len(subjectyenc) == 1:
                    hn = subjectyenc[0]
                else:
                    # 3rd: look for extensions in list
                    extlist = ["par2", "PAR2", "rar", "txt", "nfo", "nzb", "sfo", "jpg", "mkv", "txt"]
                    hn = None
                    for ext in extlist:
                        gg = re.search(r'[^ "]*.[.]' + ext, subjectyenc[0])
                        try:
                            hn = gg.group()
                            break
                        except Exception:
                            pass
                    if not hn:
                        hn = subjectyenc[0]
            # remove spaces, / and \ in filennames and replace with "."'''
            hn = hn.replace(" ", ".")
            hn = hn.replace("/", ".")
            hn = hn.replace("\\", ".")
        except Exception:
            continue
        for s in r:
            filelist = []
            segfound = True
            filelist_numbers = []
            i = 0
            for r0 in s:
                if r0.tag[-5:] == "group":
                    segfound = False
                    continue
                nr0 = r0.attrib["number"]
                filename = "<" + r0.text + ">"
                bytescount = int(r0.attrib["bytes"])
                if int(nr0) not in filelist_numbers:
                    bytescount0 += bytescount
                    filelist_numbers.append(int(nr0))
                    filelist.append((filename, int(nr0), bytescount))
                    i += 1
            if segfound:
                if hn not in hnlist:
                    filelist.insert(0, (age, i))
                    filedic[hn] = filelist
                    hnlist.append(hn)
                else:
                    # account for articles spread over multiple "files" in nzb
                    nr_add = 0
                    for fn_new, nr_new, bc_new in filelist:
                        is_double = False
                        for j, old_f0 in enumerate(filedic[hn]):
                            if j == 0:
                                continue
                            fn1, nr1, bc1 = old_f0
                            if nr1 == nr_new:
                                is_double = True
                                break
                        if not is_double:
                            filedic[hn].append((fn_new, nr_new, bc_new))
                            nr_add += 1
                    if nr_add > 0:
                        age_old, i_old = filedic[hn][0]
                        filedic[hn][0] = (age_old, i_old + nr_add)
                    
    filedic_new = {}
    # sort to avoid random downloading
    if filedic:
        filelist_sorted = sorted(filedic, key=str.lower)
        for fd in filelist_sorted:
            filedic_new[fd] = filedic[fd]
    return filedic_new, bytescount0


def get_inotify_events(inotify):
    events = []
    for event in inotify.read(timeout=1):
        is_created_file = False
        str0 = event.name
        flgs0 = []
        for flg in inotify_simple.flags.from_mask(event.mask):
            if ("flags.CREATE" in str(flg) or "flags.MODIFY" in str(flg)) and "flags.ISDIR" not in str(flg):
                flgs0.append(str(flg))
                is_created_file = True
        if not is_created_file:
            continue
        else:
            events.append((str0, flgs0))
    return events


def ParseNZB(cfg, dirs, mp_loggerqueue):
    setproctitle("gzbx." + os.path.basename(__file__))
    logger = setup_logger(mp_loggerqueue, __file__)
    nzbdir = dirs["nzb"]
    incompletedir = dirs["incomplete"]
    global TERMINATED
    sh = SigHandler_Parser(logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    pwdb = PWDBSender()

    cwd0 = os.getcwd()
    os.chdir(nzbdir)

    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(nzbdir, watch_flags)

    isfirstrun = True

    while not TERMINATED:
        events = get_inotify_events(inotify)
        if isfirstrun or events:  # and events not in eventslist):
            if isfirstrun:
                logger.debug(whoami() + "scanning nzb dir ...")
            else:
                logger.debug(whoami() + "got event in nzb_dir")
            for nzb in glob.glob("*.nzb"):
                if TERMINATED:
                    break
                nzb0 = nzb.split("/")[-1]
                if pwdb.exc("db_nzb_exists", [nzb0], {}):
                    logger.warning(whoami() + " NZB file " + nzb0 + " already exists in DB")
                    continue
                # replace [ and ] brackets in nzb0 / this is a bug in re!?
                if ("[" in nzb0) or ("]" in nzb0):
                    nzb0 = nzb0.replace("[", "(")
                    nzb0 = nzb0.replace("]", ")")
                    try:
                        os.rename(nzb, dirs["nzb"] + nzb0)
                    except Exception as e:
                        logger.error(whoami() + "Cannot rename NZB file " + nzb0 + " :" + str(e))
                        continue
                logger.info(whoami() + "inserting " + nzb0 + "into db")
                newnzb = pwdb.exc("db_nzb_insert", [nzb0], {})
                if newnzb:
                    logger.info(whoami() + "new NZB file " + nzb0 + " detected")
                    # update size
                    # rename nzb here to ....processed
                    filedic, bytescount = decompose_nzb(nzb0, logger)
                    if not filedic:
                        logger.warning("Could not interpret nzb " + nzb0 + ", setting to obsolete")
                        pwdb.exc("db_nzb_update_status", [nzb0, -1], {{"usefasttrack": False}})   # status "cannot queue / -1"
                    else:
                        size_gb = bytescount / (1024 * 1024 * 1024)
                        infostr = nzb0 + " / " + "{0:.3f}".format(size_gb) + " GB"
                        logger.debug(whoami() + "analysing NZB: " + infostr)
                        # insert files + articles
                        for key, items in filedic.items():
                            if TERMINATED:
                                break
                            data = []
                            fsize = 0
                            for i, it in enumerate(items):
                                if TERMINATED:
                                    break
                                if i == 0:
                                    age, nr0 = it
                                    ftype = get_file_type(key)
                                    logger.debug(whoami() + "analysing and inserting file " + key + " + articles: age=" + str(age) + " / nr=" + str(nr0)
                                                 + " / type=" + ftype)
                                    newfile = pwdb.exc("db_file_insert", [key, newnzb, nr0, age, ftype], {})
                                else:
                                    fn, no, size = it
                                    fsize += size
                                    data.append((fn, newfile, size, no, time.time()))
                            pwdb.exc("db_file_update_size", [key, fsize], {})
                            pwdb.exc("db_article_insert_many", [data], {})
                            # if there are nzbs in the download queue, pause after insert
                            if not pwdb.exc("db_nzb_are_all_nzb_idle", [], {}):
                                time.sleep(0.3)
                        logger.info(whoami() + "Added NZB: " + infostr + " to database / queue")
                        pwdb.exc("db_nzb_update_status", [nzb0, 1], {"usefasttrack": False})  # status "queued / 1"
                        logger.debug(whoami() + "Added NZB: " + infostr + " to GUI")
                        pwdb.exc("store_sorted_nzbs", [], {})
                        pwdb.exc("create_allfile_list_via_name", [nzb0, incompletedir], {})
            time.sleep(0.25)
            isfirstrun = False
        else:
            time.sleep(0.25)
    os.chdir(cwd0)
    logger.debug(whoami() + "exited!")


'''import logging
nzb = "/home/stephan/.ginzibix/nzb/4UBn5DJVdHQWZI{{CzsVRQWFxISqLU}}.nzb"
logger = logging.getLogger(__name__)
filedic, bytescount0 = decompose_nzb(nzb, logger)
for key, elem in filedic.items():
    nr = elem[0][1]
    age = elem[0][0]
    s = 0
    for i, e in enumerate(elem):
        if i == 0:
            continue
        s += e[2]
    print(key, nr, len(elem), age, s)'''

