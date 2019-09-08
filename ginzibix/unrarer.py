import re
import glob
import os
import pexpect
import time
import signal
from setproctitle import setproctitle
import sys

from ginzibix.mplogging import whoami
from threading import Thread
from ginzibix import mplogging, passworded_rars, par2lib
from ginzibix import PWDBSender, make_dirs, mpp_is_alive, mpp_join, GUI_Poller, get_cut_nzbname, get_cut_msg, get_bg_color, get_status_name_and_color,\
    clear_postproc_dirs, get_server_config, get_configured_servers, get_config_for_server, get_free_server_cfg, is_port_in_use, do_mpconnections,\
    kill_mpp

# todo:
#     unrar 2nd pass
#     direct unrar (UnrarThread_direct)

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
    for f0 in glob.glob(unpack_dir + "*") + glob.glob(unpack_dir + ".*"):
        if par2lib.check_for_rar_filetype(f0) == 1:
            is_double_packed = True
            break
    return is_double_packed, f0


def process_next_unrar_child_pass(event_idle, child, logger):
    str0 = ""
    timeout = False
    while True:
        try:
            a = child.read_nonblocking(timeout=120).decode("utf-8")
            str0 += a
        except pexpect.exceptions.EOF:
            break
        except pexpect.exceptions.TIMEOUT:
            timeout = True
            break
        except Exception as e:
            logger.warning(whoami() + str(e))
        if str0[-6:] == "[Q]uit":
            break
    if timeout:
        statmsg = "pexpect.timeout exceeded"
        status = -3
    else:
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
            elif "The specified password is incorrect" in str0:
                statmsg = "password incorrect!"
                status = -4
            else:
                statmsg = "unknown error"
                status = -3
        else:
            if "All OK" in str0:
                status = 0
                statmsg = "All OK"
    return status, statmsg, str0


class UnrarThread_direct(Thread):
    def __init__(self, retval, nzbname, verified_dir, unpack_dir, p2rarlist, pwdb, logger):
        Thread.__init__(self)
        self.daemon = True
        self.nzbname = nzbname
        self.pwdb = pwdb
        self.p2rarlist = p2rarlist
        self.status = 0
        self.status = retval




class UnrarThread_p2chain(Thread):
    def __init__(self, t_result, nzbname, p2, verified_dir, unpack_dir, pwdb, cfg, pw_file, event_idle, logger):
        Thread.__init__(self)
        self.daemon = True
        self.nzbname = nzbname
        self.event_idle = event_idle
        self.pwdb = pwdb
        self.cfg = cfg
        self.pw_file = pw_file
        # p2 in p2list = (p2, fnshort, fnfull, rarfiles)
        # r in rarfiles = (fn, md5)
        self.p2obj, self.fnshort, self.fnfull, self.rarfilesmd5 = p2
        self.rarfiles = [rf for (rf, md5) in self.rarfilesmd5]
        self.verified_dir = verified_dir
        if self.unpack_dir[:-1] != "/":
            self.unpack_dir += "/"
        self.unpack_dir = unpack_dir + self.fnshort
        self.logger = logger
        self.running = True
        self.appeared_rars = []
        # status:
        #    0 ... idle / externally stopped
        #    1 ... running (init, until stage I)
        #    2 ... /usr/bin/unrar started (stage II)
        #    3 ... finished, OK
        #    -1 .. finished, error
        #    -2 ... start from previous volume
        self.status = t_result

    def stop(self):
        self.running = False

    def run(self):
        self.status = 1   # running

        # if no p2 rarfiles -> exit
        try:
            firstrarfile = self.rarfiles[0]
        except Exception:
            self.status = -1
            sys.exit()

        # I. wait until directory is ready for step-by-step unrar (II. below)
        #    when first rar appears, check if pw protected
        #    if yes -> wait for all rars to appear, get pw, then proceed to II.
        #    if no -> goto II. immediately
        first_rar_appeared = False
        while self.running:
            rar_sortedlist0 = passworded_rars.get_sorted_rar_list(self.verified_dir)
            if not rar_sortedlist0:
                time.sleep(1)
                continue
            self.appeared_rars = [r2.split("/")[-1] for r1, r2 in rar_sortedlist0]

            first_rar_appeared = (self.appeared_rars[0] == firstrarfile)
            # if first rar did not appear -> wait for it
            if not first_rar_appeared:
                time.sleep(1)
                continue

            # first rar appeared -> pw checked?
            if not self.pwdb.exc("db_p2_get_ispw_checked", [self.fnshort], {}):
                # no -> check if pw protected
                is_pwp = passworded_rars.is_rar_password_protected(self.verified_dir, self.logger, rarname_start=firstrarfile)
                self.pwdb.exc("db_p2_set_ispw_checked", [self.fnshort, True], {})
                if is_pwp == 1:
                    self.pwdb.exc("db_p2_set_ispw", [self.fnshort, True], {})
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "rar archive " + self.fnshort + " is password protected", "warning"], {})
                    self.logger.info(whoami() + "rar archive " + self.fnshort + " is pw protected")
                    self.pwdb.exc("db_p2_set_ispw", [self.fnshort, True], {})
                elif is_pwp == -1:
                    self.pwdb.exc("db_p2_set_ispw", [self.fnshort, False], {})
                    self.logger.info(whoami() + "rar archive " + self.fnshort + " is not pw protected")
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "rar archive " + self.fnshort + " is not password protected", "warning"], {})
                elif is_pwp == -3:
                    self.pwdb.exc("db_p2_set_ispw", [self.fnshort, False], {})
                    self.logger.error(whoami() + "checked rar 1st volume, but was not accepted as such in pw test!")
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "checked rar 1st volume, but was not accepted as such in pw test!", "error"], {})
                    self.status = -1
                    sys.exit()
            else:
                is_pwp = self.pwdb.exc("db_p2_get_ispw", [self.fnshort, True], {})

            # if first rar appeared and not pw protected -> goto main unrar routine
            if not is_pwp:
                break

            # if pw protected and all rars are here -> get password and goto main unrar routine below
            if set(self.appeared_rars) == set(self.rarfiles):
                if self.pwdb.exc("db_p2_get_password", [self.fnshort], {}) == "N/A":
                    get_pw_direct0 = False
                    try:
                        get_pw_direct0 = (self.cfg["OPTIONS"]["GET_PW_DIRECTLY"].lower() == "yes")
                    except Exception as e:
                        self.logger.warning(whoami() + str(e))
                    self.logger.info(whoami() + "Trying to get password from file for rar archive " + self.fnshort)
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "trying to get password", "info"], {})
                    pw = passworded_rars.get_password(self.verified_dir, self.pw_file, self.nzbname, self.logger,
                                                      get_pw_direct=get_pw_direct0, rarname1=firstrarfile)
                    if pw:
                        self.logger.info(whoami() + "Found password " + pw + " for rar archive " + self.fnshort)
                        self.pwdb.exc("db_msg_insert", [self.nzbname, "found password " + pw, "info"], {})
                        self.pwdb.exc("db_p2_set_password", [self.fnshort, pw], {})
                else:
                    pw = self.pwdb.exc("db_p2_get_password", [self.fnshort], {})
                if not pw:
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "Provided password was not correct / no password found in PW file! ", "error"], {})
                    self.logger.error(whoami() + "Cannot find password for rar archive " + self.fnshort)
                    self.status = -1
                    sys.exit()
                break

        if not self.running:
            self.status = 0
            sys.exit()

        try:
            os.listdir(self.unpack_dir)
        except FileNotFoundError:
            os.mkdir(self.unpack_dir)

        # II. step-by-step unrarer

        # start /usr/bin/unrar
        if pw:
            cmd = "unrar x -y -o+ -vp -p" + pw + " '" + self.verified_dir + firstrarfile + "' '" + self.unpack_dir + "'"
        else:
            cmd = "unrar x -y -o+ -vp '" + self.verified_dir + firstrarfile + "' '" + self.unpack_dir + "'"
        child = pexpect.spawn(cmd)
        self.status = 2

        i = 0
        nextrarname = self.rarfiles[i]

        while self.running:
            status, statmsg, str0 = process_next_unrar_child_pass(child, self.logger)
            if status < 0:
                self.logger.info(whoami() + nextrarname + ": " + statmsg)
                self.pwdb.exc("db_msg_insert", [self.nzbname, "unrar " + nextrarname + " failed!", "error"], {})
                if status == -5:
                    self.status = -2
                else:
                    self.status = -1
                self.stop()
                continue
            else:
                # check for double packed!
                '''self.pwdb.exc("db_msg_insert", [self.nzbname, "checking for double packed rars", "info"], {})
                try:
                    child.kill(signal.SIGKILL)
                except Exception:
                    pass
                is_double_packed, fn = check_double_packed(self.unpack_dir)
                if is_double_packed:
                    self.pwdb.exc("db_msg_insert", [self.nzbname, "rars are double packed, starting unrar 2nd run", "warning"], {})
                    self.logger.debug(whoami() + "rars are double packed, starting another direct unrar thread")
                    retval = -100
                    t = UnrarThread_direct(retval, self.nzbname, self.verified_dir, self.unpack_dir, [], self.pwdb, self.logger)
                    t.start()
                    t.join()
                    status = retval'''
                # if all ok
                self.status = 3
                self.stop()
                continue

            # check if next rar from unrarer = self.rarfiles[i+1]
            try:
                gg = re.search(r"Insert disk with ", str0, flags=re.IGNORECASE)
                gend = gg.span()[1]
                nextrarname_new = str0[gend:-19]
            except Exception as e:
                self.logger.warning(whoami() + str(e) + ", unknown error")
                statmsg = "unknown error in re evalution"
                status = -1
                self.pwdb.exc("db_msg_insert", [self.nzbname, "unrar " + self.rarfiles[i] + " failed!", "error"], {})
                self.stop()
                continue
            try:
                i += 1
                nextrarname = self.rarfiles[i]
                if nextrarname != nextrarname_new:
                    raise
            except Exception:
                self.logger.error(whoami() + "cannot get next rarname: " + nextrarname + " / " + nextrarname_new)
                self.pwdb.exc("db_msg_insert", [self.nzbname, "unrar cannot get next rar file " + nextrarname_new, "error"], {})
                self.status = -1
                self.stop()
                continue
            gotnextrar = False
            while not gotnextrar and self.running:
                time.sleep(1)
                for f0 in glob.glob(self.verified_dir + "*") + glob.glob(self.verified_dir + ".*"):
                    if nextrarname == f0:
                        gotnextrar = True
            time.sleep(1)   # achtung hack!
            if not self.running:
                child.sendline("Q")
            else:
                child.sendline("C")

        try:
            child.kill(signal.SIGKILL)
        except Exception:
            pass
        self.logger.info(whoami() + "exited!")


def unrarer(verified_dir, unpack_dir, nzbname, pipe, mp_loggerqueue, cfg, pw_file):

    setproctitle("gzbx." + os.path.basename(__file__))
    logger = mplogging.setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")
    pwdb = PWDBSender()

    cwd0 = os.getcwd()
    sh = SigHandler_Unrar(cwd0, logger)
    signal.signal(signal.SIGINT, sh.sighandler_unrar)
    signal.signal(signal.SIGTERM, sh.sighandler_unrar)

    try:
        os.chdir(verified_dir)
    except FileNotFoundError:
        os.mkdir(verified_dir)

    pwdb.exc("db_nzb_update_unrar_status", [nzbname, 1], {})

    unrar_threads_started = False
    unrar_threads = []
    freekey = "xx-free1-xx"
    alldone = False

    while not TERMINATED:

        if pipe.poll(timeout=0.5):
            try:
                cmd, param = pipe.recv()
            except Exception as e:
                logger.warning(whoami() + str(e))
                continue
            # here comes cmd ifs ...
            pipe.send(True)
            continue

        if not unrar_threads_started:
            if os.listdir(verified_dir):
                p2list = pwdb.exc("db_p2_get_p2list", [nzbname], {})
                # threads for par2 rarchains
                p2rarlist = []
                thread_results = {}
                for p2 in p2list:
                    _, fnshort, _, _ = p2
                    thread_results[fnshort] = 1
                    t = UnrarThread_p2chain(thread_results[fnshort], nzbname, p2, verified_dir, unpack_dir, pwdb, cfg, pw_file, logger)
                    unrar_threads.append((t, fnshort, thread_results[fnshort], p2))
                    t.start()
                    p2rarlist.append(t.rarfiles)
                # single thread for par2-less rar chains
                thread_results[freekey] = 1
                t = UnrarThread_direct(thread_results[freekey], nzbname, verified_dir, unpack_dir, p2rarlist, pwdb, logger)
                unrar_threads.append((t, freekey, thread_results[freekey], None))
                t.start()
                unrar_threads_started = True
        else:
            # check if thread should be restarted
            threads_to_be_restarted = []
            for ur in unrar_threads:
                t, tkey, tresult, p2 = ur
                if tresult == -2:
                    t.join()
                    threads_to_be_restarted.append((p2, ur))
            if threads_to_be_restarted:
                for ttbr in threads_to_be_restarted:
                    p2, ur = ttbr
                    unrar_threads.remove(ur)
                    if p2:
                        _, fnshort, _, _ = p2
                        thread_results[fnshort] = 1
                        t = UnrarThread_p2chain(thread_results[fnshort], nzbname, p2, verified_dir, unpack_dir, pwdb, cfg, pw_file, logger)
                        unrar_threads.append((t, fnshort, thread_results[fnshort], p2))
                        t.start()
                    else:
                        thread_results[freekey] = 1
                        t = UnrarThread_direct(thread_results[freekey], nzbname, verified_dir, unpack_dir, p2rarlist, pwdb, logger)
                        unrar_threads.append((t, freekey, thread_results[freekey], None))
                        t.start()

            # check if all threads are done
            alldone = True
            for ur in unrar_threads:
                t, tkey, tresult, p2 = ur
                if tresult in [1, 2]:
                    alldone = False
                    break

        if alldone:
            break

    # final works
    allok = True
    for ur in unrar_threads:
        t, tkey, tresult, p2 = ur
        if tresult != 3:
            allok = False

    if allok:
        pwdb.exc("db_nzb_update_unrar_status", [nzbname, 2], {})
    else:
        pwdb.exc("db_nzb_update_unrar_status", [nzbname, 1], {})

    logger.info(whoami() + "exited!")
