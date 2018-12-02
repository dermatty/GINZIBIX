import os
import glob
import xml.etree.ElementTree as ET
import time
from .par2lib import get_file_type
import inotify_simple
from lxml import etree
import inspect
import signal
from .aux import PWDBSender
import re


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


lpref = __name__ + " - "

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
    for r in nzbroot:
        headers = r.attrib
        try:
            date = headers["date"]
            age = int((time.time() - float(date))/(24 * 3600))
            subject = headers["subject"]
            subjectyenc = subject.split("yEnc")
            # if we cannot split by "yenc" -> take the whole subject name and hope for the best
            if len(subjectyenc) == 1:
                hn = subjectyenc[0]
            else:
                # 2: look for extensions in list
                extlist = ["par2", "PAR2", "rar", "txt", "nfo", "nzb", "sfo"]
                hn = None
                for ext in extlist:
                    gg = re.search(r'[^ "]*.[.]' + ext, subjectyenc[0])
                    try:
                        hn = gg.group()
                        break
                    except Exception as e:
                        pass
                if not hn:
                    hn = subjectyenc[0]
            # remove spaces, / and \ in filennames and replace with "."
            hn = hn.replace(" ", ".")
            hn = hn.replace("/", ".")
            hn = hn.replace("\\", ".")
        except Exception as e:
            continue
        for s in r:
            filelist = []
            segfound = True
            filelist_numbers = []
            for i, r0 in enumerate(s):
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
            i -= 1
            if segfound:
                filelist.insert(0, (age, int(nr0)))
                filedic[hn] = filelist
    return filedic, bytescount0


def get_inotify_events(inotify):
    events = []
    for event in inotify.read(timeout=1):
        is_created_file = False
        str0 = event.name
        flgs0 = []
        for flg in inotify_simple.flags.from_mask(event.mask):
            if "flags.CREATE" in str(flg) and "flags.ISDIR" not in str(flg):
                flgs0.append(str(flg))
                is_created_file = True
        if not is_created_file:
            continue
        else:
            events.append((str0, flgs0))
    return events


def ParseNZB(cfg, dirs, logger):
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
                logger.info(whoami() + "inserting " + nzb0 + "into db")
                newnzb = pwdb.exc("db_nzb_insert", [nzb0], {})
                if newnzb:
                    logger.info(whoami() + "new NZB file " + nzb0 + " detected")
                    # update size
                    # rename nzb here to ....processed
                    filedic, bytescount = decompose_nzb(nzb, logger)
                    if not filedic:
                        logger.warning("Could not interpret nzb " + nzb0 + ", setting to obsolete")
                        pwdb.exc("db_nzb_update_status", [nzb0, -2], {})   # status "cannot queue / -2"
                    else:
                        size_gb = bytescount / (1024 * 1024 * 1024)
                        infostr = nzb0 + " / " + "{0:.3f}".format(size_gb) + " GB"
                        logger.debug(whoami() + "analysing NZB: " + infostr)
                        # insert files + articles
                        for key, items in filedic.items():
                            if TERMINATED:
                                break
                            data = []
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
                                    data.append((fn, newfile, size, no, time.time()))
                            pwdb.exc("db_article_insert_many", [data], {})
                            # if there are nzbs in the download queue, pause after insert 
                            if not pwdb.exc("db_nzb_are_all_nzb_idle", [], {}):
                                time.sleep(0.3)
                        logger.info(whoami() + "Added NZB: " + infostr + " to database / queue")
                        pwdb.exc("db_nzb_update_status", [nzb0, 1], {})         # status "queued / 1"
                        logger.debug(whoami() + "Added NZB: " + infostr + " to GUI")
                        pwdb.exc("store_sorted_nzbs", [], {})
                        pwdb.exc("create_allfile_list_via_name", [nzb0, incompletedir], {})
            time.sleep(0.2)
            isfirstrun = False
    os.chdir(cwd0)
    logger.debug(whoami() + "exited!")


'''import logging
nzb = "/home/stephan/.ginzibix/nzb/Das.Boot.2018.S01E01.GERMAN.720p.HDTV.x264-ACED.nzb"
logger = logging.getLogger(__name__)
filedic, bytescount0 = decompose_nzb(nzb, logger)
# print(bytescount0, filedic)
print("-" * 60)
nzb = "/home/stephan/.ginzibix/nzb_bak/Die.Schtis.in.Paris.Eine.Familie.auf.Abwegen.German.2018.AC3.BDRip.x264-iNKLUSiON.nzb"
filedic, bytescount0 = decompose_nzb(nzb, logger)
print("-" * 60)
nzb = "/home/stephan/.ginzibix/nzb_bak/Das.Boot.2018.S01E01.GERMAN.1080p.HDTV.h264.INTERNAL-ACED-xpost.nzb"
filedic, bytescount0 = decompose_nzb(nzb, logger)'''
