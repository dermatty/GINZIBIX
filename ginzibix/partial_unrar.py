import re
import glob
import os
import pexpect
import time
import signal
from setproctitle import setproctitle
import sys

from ginzibix.mplogging import whoami
from ginzibix import mplogging, passworded_rars, par2lib
from ginzibix import PWDBSender, make_dirs, mpp_is_alive, mpp_join, GUI_Poller, get_cut_nzbname, get_cut_msg, get_bg_color, get_status_name_and_color,\
    clear_postproc_dirs, get_server_config, get_configured_servers, get_config_for_server, get_free_server_cfg, is_port_in_use, do_mpconnections,\
    kill_mpp


TERMINATED = False


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


def check_double_packed(unpack_dir):
    is_double_packed = False
    for f0 in glob.glob(unpack_dir + "*"):
        if par2lib.check_for_rar_filetype(f0) == 1:
            is_double_packed = True
            break
    return is_double_packed, f0


def get_rar_files(directory):
    rarlist = []
    for rarf in glob.glob("*.rar"):
        gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
        rarlist.append((int(gg.group().split(".")[0]), rarf))
    return rarlist


def delete_all_rar_files(unpack_dir, logger):
    for f0 in glob.glob(unpack_dir + "*"):
        if par2lib.check_for_rar_filetype(f0) == 1:
            try:
                os.remove(f0)
            except Exception:
                break


def process_next_unrar_child_pass(event_idle, child, logger):
    # event_idle.set()
    str0 = ""
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
    status = 1
    statmsg = ""
    if "WARNING: You need to start extraction from a previous volume" in str0:
        child.close(force=True)
        statmsg = "WARNING: You need to start extraction from a previous volume"
        status = -5
    elif "error" in str0:
        if "packed data checksum" in str0:
            statmsg = "packed data checksum error (= corrupt rar!)"
            status = -1
        elif "- checksum error" in str0:
            statmsg = "checksum error (= rar is missing!)"
            status = -2
        else:
            statmsg = "unknown error"
            status = -3
    else:
        if "All OK" in str0:
            status = 0
            statmsg = "All OK"
    #event_idle.clear()
    return status, statmsg, str0


def partial_unrar(directory, unpack_dir, nzbname, mp_loggerqueue, password, event_idle, cfg):

    setproctitle("gzbx." + os.path.basename(__file__))
    logger = mplogging.setup_logger(mp_loggerqueue, __file__)
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
        rar_sortedlist0 = passworded_rars.get_sorted_rar_list(directory)
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
        status = 1
        child = pexpect.spawn(cmd)
        status, statmsg, str0 = process_next_unrar_child_pass(event_idle, child, logger)
        if status < 0:
            logger.info(whoami() + nextrarname + ": " + statmsg)
            pwdb.exc("db_msg_insert", [nzbname, "unrar " + nextrarname + " failed!", "error"], {})
        else:
            pwdb.exc("db_msg_insert", [nzbname, "checking for double packed rars", "info"], {})
            # check if double packed
            try:
                child.kill(signal.SIGKILL)
            except Exception:
                pass
            is_double_packed, fn = check_double_packed(unpack_dir)
            if is_double_packed:
                pwdb.exc("db_msg_insert", [nzbname, "rars are double packed, starting unrar 2nd run", "warning"], {})
                logger.debug(whoami() + "rars are double packed, executing " + cmd)
                # unrar without pausing! 
                cmd = "unrar x -y -o+ -p" + password + " '" + fn + "' '" + unpack_dir + "'"
                child = pexpect.spawn(cmd)
                status, statmsg, str0 = process_next_unrar_child_pass(event_idle, child, logger)
                if status < 0:
                    logger.info(whoami() + "2nd pass: " + statmsg)
                    pwdb.exc("db_msg_insert", [nzbname, "unrar 2nd pass failed!", "error"], {})
                elif status == 0:
                    statmsg = "All OK"
                    status = 0
                    pwdb.exc("db_msg_insert", [nzbname, "unrar success 2nd pass for all rar files!", "info"], {})
                    logger.info(whoami() + "unrar success 2nd pass for all rar files!")
                    logger.debug(whoami() + "deleting all rar files in unpack_dir")
                    delete_all_rar_files(unpack_dir, logger)
                elif status == 1:
                    status = -3
                    statmsg = "unknown error"
                    logger.info(whoami() + "2nd pass: " + statmsg + " / " + str0)
                    pwdb.exc("db_msg_insert", [nzbname, "unrar 2nd pass failed!", "error"], {})
            else:
                statmsg = "All OK"
                status = 0
                pwdb.exc("db_msg_insert", [nzbname, "unrar success for all rar files!", "info"], {})
                logger.info(whoami() + "unrar success for all rar files!")
    else:
        cmd = "unrar x -y -o+ -vp '" + directory + nextrarname + "' '" + unpack_dir + "'"
        logger.debug(whoami() + "rar archive is NOT passworded, executing " + cmd)
        pwdb.exc("db_msg_insert", [nzbname, "unraring rar archive", "info"], {})

        child = pexpect.spawn(cmd)
        status = 1      # 1 ... running, 0 ... exited ok, -1 ... rar corrupt, -2 ..missing rar, -3 ... unknown error

        while not TERMINATED:
            oldnextrarname = nextrarname.split("/")[-1]
            status, statmsg, str0 = process_next_unrar_child_pass(event_idle, child, logger)
            if status < 0:
                logger.info(whoami() + nextrarname + ": " + statmsg)
                pwdb.exc("db_msg_insert", [nzbname, "unrar " + oldnextrarname + " failed!", "error"], {})
                break
            logger.info(whoami() + nextrarname + ": unrar success!")
            if status == 0:
                pwdb.exc("db_msg_insert", [nzbname, "checking for double packed rars", "info"], {})
                # check if double packed
                try:
                    child.kill(signal.SIGKILL)
                except Exception:
                    pass
                is_double_packed, fn = check_double_packed(unpack_dir)
                if is_double_packed:
                    pwdb.exc("db_msg_insert", [nzbname, "rars are double packed, starting unrar 2nd run", "warning"], {})
                    cmd = "unrar x -y -o+ '" + fn + "' '" + unpack_dir + "'"
                    logger.debug(whoami() + "rars are double packed, executing " + cmd)
                    # unrar without pausing! 
                    child = pexpect.spawn(cmd)
                    status, statmsg, str0 = process_next_unrar_child_pass(event_idle, child, logger)
                    if status < 0:
                        logger.info(whoami() + "2nd pass: " + statmsg)
                        pwdb.exc("db_msg_insert", [nzbname, "unrar 2nd pass failed!", "error"], {})
                        break
                    if status == 0:
                        statmsg = "All OK"
                        status = 0
                        pwdb.exc("db_msg_insert", [nzbname, "unrar success 2nd pass for all rar files!", "info"], {})
                        logger.info(whoami() + "unrar success 2nd pass for all rar files!")
                        logger.debug(whoami() + "deleting all rar files in unpack_dir")
                        delete_all_rar_files(unpack_dir, logger)
                        break
                    if status == 1:
                        status = -3
                        statmsg = "unknown error"
                        logger.info(whoami() + "2nd pass: " + statmsg + " / " + str0)
                        pwdb.exc("db_msg_insert", [nzbname, "unrar 2nd pass failed!", "error"], {})
                        break
                else:
                    statmsg = "All OK"
                    status = 0
                    pwdb.exc("db_msg_insert", [nzbname, "unrar success for all rar files!", "info"], {})
                    logger.info(whoami() + "unrar success for all rar files!")
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
            # first, if .r00
            try:
                nextrar_number = int(re.search(r"\d{0,9}$", nextrarname).group())
                nextrar_wo_number = nextrarname.rstrip("0123456789") 
            except Exception:
                nextrar_wo_number = nextrarname
                nextrar_number = -1
            # if part01.rar
            if nextrar_number == -1:
                try:
                    nextrar_number = int(nextrarname.split(".part")[-1].split(".rar")[0])
                    nextrar_wo_number = nextrarname.rstrip(".rar").rstrip("0123456789").rstrip("part").rstrip(".")
                except Exception:
                    nextrar_wo_number = nextrarname
                    nextrar_number = -1
            gotnextrar = False
            nextrar_short = nextrarname.split("/")[-1]
            # todo: hier deadlock/unendliches Warten im Postprocess vermeiden, wenn rar nicht auftaucht!
            event_idle.set()
            while not gotnextrar and not TERMINATED:
                time.sleep(1)
                for f0 in glob.glob(directory + "*"):
                    if nextrarname == f0:
                        gotnextrar = True
                # now check if we waited too long for next rar0 - but how?
                if not gotnextrar and nextrar_number != -1:
                    for f0 in glob.glob(directory + "*"):
                        f0_number = -1
                        f0_wo_number = f0
                        try:
                            f0_number = int(re.search(r"\d{0,9}$", f0).group())
                            f0_wo_number = f0.rstrip("0123456789")
                        except Exception:
                            try:
                                f0_number = int(f0.split(".part")[-1].split(".rar")[0])
                                f0_wo_number = f0.rstrip(".rar").rstrip("0123456789").rstrip("part").rstrip(".")
                            except Exception:
                                continue
                        if f0_number == -1:
                            continue
                        if f0_wo_number == nextrar_wo_number and f0_number > nextrar_number:
                            pwdb.exc("db_msg_insert", [nzbname, "unrar waiting for next rar, but next rar " + nextrar_short + " seems to be skipped, you may want to interrupt ...", "warning"], {})
                            break

            event_idle.clear()
            if TERMINATED:
                break
            time.sleep(1)   # achtung hack!
            child.sendline("C")

    try:
        child.kill(signal.SIGKILL)
    except Exception:
        pass
    if TERMINATED:
        logger.info(whoami() + "exited!")
    else:
        logger.info(whoami() + str(status) + " " + statmsg)
        try:
            os.chdir(cwd0)
            if status == 0:
                pwdb.exc("db_nzb_update_unrar_status", [nzbname, 2], {})
            elif status == -5:
                pwdb.exc("db_nzb_update_unrar_status", [nzbname, -2], {})
            else:
                pwdb.exc("db_nzb_update_unrar_status", [nzbname, -1], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
        event_idle.clear()
        logger.info(whoami() + "exited!")
    sys.exit()
