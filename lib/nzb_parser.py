import os
import glob
import xml.etree.ElementTree as ET
import time
import re
# import logging
import inotify_simple
# from gpeewee import PWDB


lpref = __name__ + " - "


def decompose_nzb(nzb, logger):
    try:
        tree = ET.parse(nzb)
        logger.info(lpref + "downloading NZB file " + nzb)
    except Exception as e:
        logger.error(lpref + str(e) + ": cannot download NZB file " + nzb)
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


def get_file_type(filename):
    if re.search(r"[.]rar$", filename, flags=re.IGNORECASE):
        filetype0 = "rar"
    elif re.search(r"[.]nfo$", filename, flags=re.IGNORECASE):
        filetype0 = "nfo"
    elif re.search(r"[.]sfv$", filename, flags=re.IGNORECASE):
        filetype0 = "sfv"
    elif re.search(r"[.]par2$", filename, flags=re.IGNORECASE):
        if re.search(r"vol[0-9][0-9]*[+]", filename, flags=re.IGNORECASE):
            filetype0 = "par2vol"
        else:
            filetype0 = "par2"
    else:
        filetype0 = "etc"
    return filetype0


def ParseNZB(pwdb, nzbdir, logger):
    cwd0 = os.getcwd()
    os.chdir(nzbdir)

    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(nzbdir, watch_flags)

    isfirstrun = True

    while True:
        events = get_inotify_events(inotify)
        if isfirstrun or events:  # and events not in eventslist):
            logger.debug(lpref + "got nzb event")
            for nzb in glob.glob("*.nzb"):
                nzb0 = nzb.split("/")[-1]
                if pwdb.db_nzb_exists(nzb0):
                    logger.warning(lpref + " NZB file " + nzb0 + " already exists in DB")
                    continue
                with pwdb.db.atomic():
                    logger.info(lpref + "inserting " + nzb0 + "into db")
                    newnzb = pwdb.db_nzb_insert(nzb0)
                    if newnzb:
                        logger.info(lpref + "new NZB file " + nzb0 + " detected")
                        # update size
                        # rename nzb here to ....processed
                        filedic, bytescount = decompose_nzb(nzb, logger)
                        size_gb = bytescount / (1024 * 1024 * 1024)
                        infostr = nzb0 + " / " + "{0:.3f}".format(size_gb) + " GB"
                        logger.debug(lpref + "analysing NZB: " + infostr)
                        # insert files + articles
                        for key, items in filedic.items():
                            data = []
                            for i, it in enumerate(items):
                                if i == 0:
                                    age, nr0 = it
                                    ftype = get_file_type(key)
                                    logger.debug(lpref + "analysing and inserting file " + key + " + articles: age=" + str(age) + " / nr=" + str(nr0)
                                                 + " / type=" + ftype)
                                    newfile = pwdb.db_file_insert(key, newnzb, nr0, age, ftype)
                                else:
                                    fn, no, size = it
                                    data.append((fn, newfile, size, no, time.time()))
                            pwdb.db_article_insert_many(data)
                        logger.info(lpref + "Added NZB: " + infostr)
            isfirstrun = False
    logger.warning(lpref + "exiting")
    os.chdir(cwd0)


'''nzbdir = "/home/stephan/.ginzibix/nzb/"
logger = logging.getLogger(__name__)

pwdb = PWDB(logger)
pwdb.db_nzb_deleteall()
pwdb.db_file_deleteall()
pwdb.db_article_deleteall()

ParseNZB(nzbdir, pwdb, logger)
print(80 * "-")
for d in pwdb.db_nzb_getall():
    print(d)
print(80 * "-")
for d in pwdb.db_file_getall():
    print(d)
print(80 * "-")
for d in pwdb.db_article_getall():
    print(d)
pwdb.db_drop()
pwdb.db_close()'''
