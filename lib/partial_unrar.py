import re
import glob
import os
import pexpect
import time
import signal
from .passworded_rars import get_sorted_rar_list
from .aux import PWDBSender
from .mplogging import setup_logger, whoami


TERMINATED = False

lpref = __name__ + " - "


class SigHandler_Unrar:
    def __init__(self, wd, logger):
        self.wd = wd
        self.logger = logger

    def sighandler_unrar(self, a, b):
        global TERMINATED
        try:
            os.chdir(self.wd)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


def get_rar_files(directory):
    rarlist = []
    for rarf in glob.glob("*.rar"):
        gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
        rarlist.append((int(gg.group().split(".")[0]), rarf))
    return rarlist


def partial_unrar(directory, unpack_dir, nzbname, mp_loggerqueue, password, event_idle, cfg):
    logger = setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")
    pwdb = PWDBSender()

    event_idle.clear()

    cwd0 = os.getcwd()
    sh = SigHandler_Unrar(cwd0, logger)
    signal.signal(signal.SIGINT, sh.sighandler_unrar)
    signal.signal(signal.SIGTERM, sh.sighandler_unrar)

    try:
        os.chdir(directory)
    except FileNotFoundError:
        os.mkdir(directory)

    logger.info(whoami() + "started partial_unrar")
    # get already present rar files
    rar_sortedlist0 = None
    while not TERMINATED:
        rar_sortedlist0 = get_sorted_rar_list(directory)
        # todo: what to do if does not finish here?
        if rar_sortedlist0:
            break
        time.sleep(1)

    if TERMINATED:
        logger.info(whoami() + "terminated!")
        return
    rar_sortedlist = []
    for r1, r2 in rar_sortedlist0:
        try:
            rar_sortedlist.append((r1, r2.split("/")[-1]))
        except Exception as e:
            logger.debug(whoami() + whoami() + ": " + str(e))

    pwdb.exc("db_nzb_update_unrar_status", [nzbname, 1], {})
    nextrarname = rar_sortedlist[0][1]
    # first valid rar_sortedlist in place, start unrar!
    if password:
        cmd = "unrar x -y -o+ -p" + password + " '" + directory + nextrarname + "' '" + unpack_dir + "'"
        logger.debug(whoami() + "rar archive is passworded, executing " + cmd)
        pwdb.exc("db_msg_insert", [nzbname, "unraring pw protected rar archive", "info"], {})
    else:
        cmd = "unrar x -y -o+ -vp '" + directory + nextrarname + "' '" + unpack_dir + "'"
        logger.debug(whoami() + "rar archive is NOT passworded, executing " + cmd)
        pwdb.exc("db_msg_insert", [nzbname, "unraring rar archive", "info"], {})

    child = pexpect.spawn(cmd)
    status = 1      # 1 ... running, 0 ... exited ok, -1 ... rar corrupt, -2 ..missing rar, -3 ... unknown error
    while not TERMINATED:
        oldnextrarname = nextrarname.split("/")[-1]
        str0 = ""
        event_idle.set()
        while True:
            try:
                a = child.read_nonblocking(timeout=120).decode("utf-8")
                str0 += a
            except pexpect.exceptions.EOF:
                break
            except Exception as e:
                logger.warning(whoami() + str(e))
            if str0[-6:] == "[Q]uit":
                break
        event_idle.clear()
        if "WARNING: You need to start extraction from a previous volume" in str0:
            child.close(force=True)
            statmsg = "WARNING: You need to start extraction from a previous volume"
            status = -5
            break
        if "error" in str0:
            if "packed data checksum" in str0:
                statmsg = "packed data checksum error (= corrupt rar!)"
                status = -1
            elif "- checksum error" in str0:
                statmsg = "checksum error (= rar is missing!)"
                status = -2
            else:
                statmsg = "unknown error"
                status = -3
            logger.info(whoami() + nextrarname + ": " + statmsg)
            pwdb.exc("db_msg_insert", [nzbname, "unrar " + oldnextrarname + " failed!", "error"], {})
            break
        else:
            logger.info(whoami() + nextrarname + ": unrar success!")
        if "All OK" in str0:
            statmsg = "All OK"
            status = 0
            pwdb.exc("db_msg_insert", [nzbname, "unrar success for all rar files!", "info"], {})
            break
        try:
            gg = re.search(r"Insert disk with ", str0, flags=re.IGNORECASE)
            gend = gg.span()[1]
            nextrarname = str0[gend:-19]
        except Exception as e:
            logger.warning(whoami() + str(e) + ", unknown error")
            statmsg = "unknown error in re evalution"
            status = -4
            pwdb.exc("db_msg_insert", [nzbname, "unrar " + oldnextrarname + " failed!", "error"], {})
            break
        pwdb.exc("db_msg_insert", [nzbname, "unrar " + oldnextrarname + " success!", "info"], {})
        logger.debug(whoami() + "Waiting for next rar: " + nextrarname)
        gotnextrar = False
        # todo: hier deadlock/unendliches Warten im Postprocess vermeiden, wenn rar nicht auftaucht!
        event_idle.set()
        while not gotnextrar and not TERMINATED:
            time.sleep(1)
            for f0 in glob.glob(directory + "*"):
                if nextrarname == f0:
                    try:
                        gotnextrar = True
                        break
                    except Exception as e:
                        logger.warning(whoami() + str(e))
        event_idle.clear()
        if TERMINATED:
            child.kill(signal.SIGKILL)
            break
        time.sleep(1)   # achtung hack!
        child.sendline("C")
    if TERMINATED:
        logger.info(whoami() + "exited!")
    else:
        logger.info(whoami() + str(status) + " " + statmsg)
        os.chdir(cwd0)
        if status == 0:
            pwdb.exc("db_nzb_update_unrar_status", [nzbname, 2], {})
        elif status == -5:
            pwdb.exc("db_nzb_update_unrar_status", [nzbname, -2], {})
        else:
            pwdb.exc("db_nzb_update_unrar_status", [nzbname, -1], {})
