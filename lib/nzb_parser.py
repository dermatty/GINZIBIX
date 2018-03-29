import os
import glob
import xml.etree.ElementTree as ET
import time
import logging
import inotify_simple
from gpeewee import db_nzb_insert, db_nzb_update_size, db_nzb_getall, db_close, db_file_insert, db_file_getall, db_drop, db_article_insert
from gpeewee import db_article_getall, db_nzb_deleteall, db_file_deleteall, db_article_deleteall, db_nzb_exists


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def decompose_nzb(nzb):
    try:
        tree = ET.parse(nzb)
        logger.info("Downloading NZB file: " + nzb)
    except Exception as e:
        logger.error(str(e) + ": please provide at least 1 NZB file!")
        return None
    nzbroot = tree.getroot()
    filedic = {}
    bytescount0 = 0
    for r in nzbroot:
        headers = r.attrib
        try:
            date = headers["date"]
            age = int((time.time() - float(date))/(24 * 3600))
            subject = headers["subject"]
            hn_list = subject.split('"')
            hn = hn_list[1]
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
                bytescount0 += bytescount
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


def ParseNZB(nzbdir):
    cwd0 = os.getcwd()
    os.chdir(nzbdir)

    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(nzbdir, watch_flags)

    isfirstrun = True

    while True:
        events = get_inotify_events(inotify)
        if isfirstrun or events:  # and events not in eventslist):
            for nzb in glob.glob("*.nzb"):
                nzb0 = nzb.split("/")[-1]
                if db_nzb_exists(nzb0):
                    continue
                newnzb = db_nzb_insert(nzb0, 0)
                if newnzb:
                    # update size
                    # rename nzb here to ....processed
                    filedic, bytescount = decompose_nzb(nzb)
                    db_nzb_update_size(nzb0, bytescount)
                    logger.warning("Analysing NZB: " + nzb0 + " / bytes=" + str(bytescount))
                    # insert files + articles
                    for key, items in filedic.items():
                        for i, it in enumerate(items):
                            if i == 0:
                                age, nr0 = it
                                logger.warning("Analysing file " + key + " + articles: age=" + str(age) + " / nr=" + str(nr0))
                                newfile = db_file_insert(key, newnzb, nr0, age)
                            else:
                                fn, no, size = it
                                db_article_insert(fn, newfile, size, no)
            isfirstrun = False

    os.chdir(cwd0)


nzbdir = "/home/stephan/.ginzibix/nzb/"
db_nzb_deleteall()
db_file_deleteall()
db_article_deleteall()

ParseNZB(nzbdir)
print(80 * "-")
for d in db_nzb_getall():
    print(d)
print(80 * "-")
for d in db_file_getall():
    print(d)
print(80 * "-")
for d in db_article_getall():
    print(d)
db_drop()
db_close()
