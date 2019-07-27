import os
import shutil
import glob
import inotify_simple
import signal
import time
from setproctitle import setproctitle

from ginzibix.mplogging import whoami
from ginzibix import mplogging, par2lib
from ginzibix import PWDBSender, make_dirs, mpp_is_alive, mpp_join, GUI_Poller, get_cut_nzbname, get_cut_msg, get_bg_color, get_status_name_and_color,\
    clear_postproc_dirs, get_server_config, get_configured_servers, get_config_for_server, get_free_server_cfg, is_port_in_use, do_mpconnections,\
    kill_mpp


TERMINATED = False


class SigHandler_Renamer:
    def __init__(self, logger):
        self.logger = logger

    def sighandler_renamer(self, a, b):
        self.logger.info(whoami() + "terminating ...")
        global TERMINATED
        TERMINATED = True
        # sys.exit()


def get_not_yet_renamed_files(source_dir, filewrite_lock):
    nrf = []
    with filewrite_lock:
        for fn in glob.glob(source_dir + "*"):
            fn0 = fn.split("/")[-1]
            if not fn0.endswith(".renamed"):
                nrf.append((fn0, par2lib.calc_file_md5hash_16k(fn0)))
    return nrf


def scan_renamed_dir(renamed_dir, p2obj, filewrite_lock, logger):
    # get all files in renamed
    p2list = []
    with filewrite_lock:
        rn = []
        p2obj0 = p2obj
        p2basename0 = None
        for fn in glob.glob(renamed_dir + "*"):
            fn0 = fn.split("/")[-1]

            ptype0 = par2lib.check_for_par_filetype(fn)
            if ptype0 == 1:
                p2obj0 = par2lib.Par2File(fn)
                p2basename0 = fn.split(".par2")[0]
                logger.debug(whoami() + "Found .par2 in _renamed0: " + fn0)
                p2list.append((p2obj0, p2basename0))
            rn.append(fn0)
    return rn, p2obj0, p2basename0


def scan_for_par2(notrenamedfiles, logger):
    p2obj0 = None
    p2basename0 = None
    for fn, _ in notrenamedfiles:
        ptype0 = par2lib.check_for_par_filetype(fn)
        if ptype0 == 1:
            p2obj0 = par2lib.Par2File(fn)
            p2basename0 = fn.split(".par2")[0]
            logger.debug(whoami() + "Found .par2 in _downloaded0: " + fn.split("/")[-1])
            break
    return p2obj0, p2basename0


def renamer_process_par2s(source_dir, dest_dir, p2obj, p2basename, notrenamedfiles, pwdb, renamer_result_queue, filewrite_lock):
    # search for not yet renamed par2/vol files
    p2obj0 = p2obj
    p2basename0 = p2basename
    not_renamed_par2list = []
    for pname, phash in notrenamedfiles:
        ptype0 = par2lib.check_for_par_filetype(pname)
        if ptype0 == 1:
            ptype = "par2"
            p2obj0 = par2lib.Par2File(pname)
            p2basename0 = pname.split(".par2")[0]
        elif ptype0 == 2:
            ptype = "par2vol"
        else:
            continue
        not_renamed_par2list.append((pname, ptype, phash))
    if not_renamed_par2list:
        for pname, ptype, phash in not_renamed_par2list:
            pp = (pname, phash)
            # oldft = pwdb.db_file_get_orig_filetype(pname)
            oldft = pwdb.exc("db_file_get_orig_filetype", [pname], {})
            if ptype == "par2":
                with filewrite_lock:
                    shutil.copyfile(source_dir + pname, dest_dir + pname)
                pwdb.exc("db_file_set_renamed_name", [pname, pname], {})
                # pwdb.db_file_set_renamed_name(pname, pname)
                # pwdb.db_file_set_file_type(pname, "par2")
                pwdb.exc("db_file_set_file_type", [pname, "par2"], {})
                renamer_result_queue.put((pname, dest_dir + pname, "par2", pname, oldft))
                # os.rename(source_dir + pname, source_dir + pname + ".renamed")
                with filewrite_lock:
                    os.remove(source_dir + pname)
                notrenamedfiles.remove(pp)
            elif ptype == "par2vol" and p2basename0:
                # todo: if not p2basename ??
                #volpart1 = randint(1, 99)
                #volpart2 = randint(1, 99)
                #pname2 = p2basename0 + ".vol" + str(volpart1).zfill(3) + "+" + str(volpart2).zfill(3) + ".PAR2"
                with filewrite_lock:
                    shutil.copyfile(source_dir + pname, dest_dir + pname)
                pwdb.exc("db_file_set_renamed_name", [pname, pname], {})
                pwdb.exc("db_file_set_file_type", [pname, "par2vol"], {})
                renamer_result_queue.put((pname, dest_dir + pname, "par2vol", pname, oldft))
                with filewrite_lock:
                    os.remove(source_dir + pname)
                notrenamedfiles.remove(pp)
    return p2obj0, p2basename0


def rename_and_move_rarandremainingfiles_old(p2obj, notrenamedfiles, source_dir, dest_dir, pwdb,
                                             renamer_result_queue, filewrite_lock, logger):
    if p2obj:
        rarfileslist = [(fn, md5) for fn, md5 in p2obj.md5_16khash() if par2lib.get_file_type(fn) == "rar"]
        notrenamedfiles0 = notrenamedfiles[:]
        # rarfiles
        for a_name, a_md5 in notrenamedfiles0:
            pp = (a_name, a_md5)
            try:
                r_name = [fn for fn, r_md5 in rarfileslist if r_md5 == a_md5][0]
                if not r_name:
                    continue
                if r_name != a_name:
                    with filewrite_lock:
                        shutil.copyfile(source_dir + a_name, dest_dir + r_name)
                else:
                    with filewrite_lock:
                        shutil.copyfile(source_dir + a_name, dest_dir + a_name)
                    r_name = a_name
                # oldft = pwdb.db_file_get_orig_filetype(a_name)
                oldft = pwdb.exc("db_file_get_orig_filetype", [a_name], {})
                # pwdb.db_file_set_renamed_name(a_name, r_name)
                pwdb.exc("db_file_set_renamed_name", [a_name, r_name], {})
                pwdb.exc("db_file_set_file_type", [a_name, "rar"], {})
                renamer_result_queue.put((r_name, dest_dir + r_name, "rar", a_name, oldft))
                # os.rename(source_dir + a_name, source_dir + a_name + ".renamed")
                with filewrite_lock:
                    os.remove(source_dir + a_name)
                notrenamedfiles.remove(pp)
            except IndexError:
                pass
            except Exception as e:
                logger.warning(whoami() + str(e))
    for a_name, a_md5 in notrenamedfiles:
        with filewrite_lock:
            shutil.copyfile(source_dir + a_name, dest_dir + a_name)
        ft = par2lib.get_file_type(a_name)
        # pwdb.db_file_set_renamed_name(a_name, a_name)
        pwdb.exc("db_file_set_renamed_name", [a_name, a_name], {})
        # pwdb.db_file_set_file_type(a_name, ft)
        pwdb.exc("db_file_set_file_type", [a_name, ft], {})
        renamer_result_queue.put((a_name, dest_dir + a_name, ft, a_name, ft))
        # os.rename(source_dir + a_name, source_dir + a_name + ".renamed")
        with filewrite_lock:
            os.remove(source_dir + a_name)


def get_inotify_events(inotify, filewrite_lock):
    events = []
    with filewrite_lock:
        for event in inotify.read(timeout=0.3):
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


# renamer with inotify
def renamer_old(child_pipe, renamer_result_queue, mp_loggerqueue, filewrite_lock):
    setproctitle("gzbx." + os.path.basename(__file__))

    logger = mplogging.setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting renamer process")
    
    sh = SigHandler_Renamer(logger)
    signal.signal(signal.SIGINT, sh.sighandler_renamer)
    signal.signal(signal.SIGTERM, sh.sighandler_renamer)

    pwdb = PWDBSender()
    cwd0 = os.getcwd()

    while not TERMINATED:
        # wait for start command
        logger.debug(whoami() + "waiting for start command")
        while not TERMINATED:
            if child_pipe.poll():
                command = child_pipe.recv()
                try:
                    cmd0, source_dir, dest_dir = command
                    if cmd0 == "start":
                        break
                except Exception as e:
                    logger.warning(whoami() + str(e))
            time.sleep(0.1)

        if TERMINATED:
            break

        if source_dir[-1] != "/":
            source_dir += "/"
        if dest_dir[-1] != "/":
            dest_dir += "/"

        try:
            os.chdir(dest_dir)
        except FileNotFoundError:
            os.mkdir(dest_dir)

        os.chdir(source_dir)

        # init inotify
        inotify = inotify_simple.INotify()
        watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
        wd = inotify.add_watch(source_dir, watch_flags)

        p2obj = None
        p2basename = None

        # eventslist = []
        isfirstrun = True

        while not TERMINATED:
            events = get_inotify_events(inotify, filewrite_lock)
            if isfirstrun or events:  # and events not in eventslist):
                logger.debug(whoami() + "Events: " + str(events))
                # get all files not yet .renamed
                logger.debug(whoami() + "reading not yet downloaded files in _downloaded0")
                notrenamedfiles = get_not_yet_renamed_files(source_dir, filewrite_lock)
                # get all renames filed & trying to get .par2 file
                logger.debug(whoami() + "Reading files in _renamed0 & trying to get .par2 file")
                renamedfiles, p2obj, p2basename = scan_renamed_dir(dest_dir, p2obj, filewrite_lock, logger)
                # if no par2 in _renamed, check _downloaded0
                if not p2obj:
                    logger.debug(whoami() + "No p2obj yet found, looking in _downloaded0")
                    p2obj, p2basename = scan_for_par2(notrenamedfiles, logger)
                    if p2obj:
                        logger.debug(whoami() + "p2obj found: " + p2basename)
                # rename par2 and move them
                p2obj, p2objname = renamer_process_par2s(source_dir, dest_dir, p2obj, p2basename, notrenamedfiles, pwdb,
                                                         renamer_result_queue, filewrite_lock)
                # rename & move rar + remaining files
                rename_and_move_rarandremainingfiles_old(p2obj, notrenamedfiles, source_dir, dest_dir, pwdb, renamer_result_queue,
                                                         filewrite_lock, logger)
                isfirstrun = False
            if child_pipe.poll():
                command, _, _ = child_pipe.recv()
                if command == "pause":
                    break
        os.chdir(cwd0)
        try:
            if wd:
                inotify.rm_watch(wd)
        except Exception as e:
            logger.warning(whoami() + str(e))
        logger.debug(whoami() + "renamer paused")
    logger.info(whoami() + "exited!")


# ---------------------------------------------------------------------------------------------------------
#
# NEW VERSION
#
# ---------------------------------------------------------------------------------------------------------

def rename_and_move_rarandremainingfiles(p2list, notrenamedfiles, source_dir, dest_dir, pwdb, renamer_result_queue,
                                         filewrite_lock, logger):
    for _, _, _, rarfileslist in p2list:
        notrenamedfiles0 = notrenamedfiles[:]
        # rarfiles
        for fullname, shortname, a_md5 in notrenamedfiles0:
            pp = (fullname, shortname, a_md5)
            try:
                r_name = [fn for fn, r_md5 in rarfileslist if r_md5 == a_md5][0]
                if not r_name:
                    continue
                if r_name != shortname:
                    with filewrite_lock:
                        shutil.copyfile(source_dir + shortname, dest_dir + r_name)
                else:
                    with filewrite_lock:
                        shutil.copyfile(source_dir + shortname, dest_dir + shortname)
                    r_name = shortname
                oldft = pwdb.exc("db_file_get_orig_filetype", [shortname], {})
                pwdb.exc("db_file_set_renamed_name", [shortname, r_name], {})
                pwdb.exc("db_file_set_file_type", [shortname, "rar"], {})
                renamer_result_queue.put((r_name, dest_dir + r_name, "rar", shortname, oldft))
                with filewrite_lock:
                    os.remove(source_dir + shortname)
                notrenamedfiles.remove(pp)
            except IndexError:
                pass
            except Exception as e:
                logger.warning(whoami() + str(e))

    for fullname, shortname, a_md5 in notrenamedfiles:
        with filewrite_lock:
            shutil.copyfile(source_dir + shortname, dest_dir + shortname)
        ft = par2lib.get_file_type(fullname, inspect=True)
        oldft = pwdb.exc("db_file_get_orig_filetype", [shortname], {})
        pwdb.exc("db_file_set_renamed_name", [shortname, shortname], {})
        pwdb.exc("db_file_set_file_type", [shortname, ft], {})
        renamer_result_queue.put((shortname, dest_dir + shortname, ft, shortname, oldft))
        with filewrite_lock:
            os.remove(source_dir + shortname)


def get_not_yet_renamed_files_new(dir0, pwdb, filewrite_lock, logger):
    nrf = []
    with filewrite_lock:
        for fn in glob.glob(dir0 + "*"):
            fn0 = fn.split("/")[-1]
            rn = pwdb.exc("db_file_get_renamed_name", [fn0], {})
            is_renamed = rn and (rn != "N/A")
            if not is_renamed:
                nrf.append((fn0, par2lib.calc_file_md5hash_16k(fn0)))
    p2list = []
    with filewrite_lock:
        for fn in glob.glob(dir0 + "*"):
            fn0 = fn.split("/")[-1]
            ptype0 = par2lib.check_for_par_filetype(fn)
            if ptype0 == 1:
                p2obj0 = par2lib.Par2File(fn)
                p2basename0 = fn.split(".par2")[0]
                logger.debug(whoami() + "Found .par2 in _renamed0: " + fn0)
                p2list.append((p2obj0, p2basename0))
    return nrf, p2list


# renamer with inotify
def renamer(child_pipe, renamer_result_queue, mp_loggerqueue, filewrite_lock):
    setproctitle("gzbx." + os.path.basename(__file__))

    logger = mplogging.setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting renamer process")

    sh = SigHandler_Renamer(logger)
    signal.signal(signal.SIGINT, sh.sighandler_renamer)
    signal.signal(signal.SIGTERM, sh.sighandler_renamer)

    pwdb = PWDBSender()
    cwd0 = os.getcwd()

    while not TERMINATED:
        # wait for start command
        logger.debug(whoami() + "waiting for start command")
        while not TERMINATED:
            if child_pipe.poll():
                command = child_pipe.recv()
                try:
                    cmd0, source_dir, dest_dir, nzbname = command
                    if cmd0 == "start":
                        break
                except Exception as e:
                    logger.warning(whoami() + str(e))
            time.sleep(0.1)

        if TERMINATED:
            break

        if source_dir[-1] != "/":
            source_dir += "/"
        if dest_dir[-1] != "/":
            dest_dir += "/"

        try:
            os.chdir(dest_dir)
        except FileNotFoundError:
            os.mkdir(dest_dir)

        os.chdir(source_dir)

        # init inotify
        inotify = inotify_simple.INotify()
        watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
        wd = inotify.add_watch(source_dir, watch_flags)

        isfirstrun = True
        p2list = pwdb.exc("db_nzb_get_p2list", [nzbname], {})

        while not TERMINATED:
            events = get_inotify_events(inotify, filewrite_lock)
            if isfirstrun or events:  # and events not in eventslist):
                logger.debug(whoami() + "Events: " + str(events))
                # in downloaded_dir: look for not renamed files
                logger.debug(whoami() + "reading not yet downloaded files in _downloaded0")
                notrenamedfiles = []
                with filewrite_lock:
                    for fnfull in glob.glob(source_dir + "*"):
                        fnshort = fnfull.split("/")[-1]
                        rn = pwdb.exc("db_file_get_renamed_name", [fnshort], {})
                        is_renamed = rn and (rn != "N/A")
                        if not is_renamed:
                            p2 = par2lib.Par2File(fnfull)
                            if p2:
                                oldft = pwdb.exc("db_file_get_orig_filetype", [fnshort], {})
                                try:
                                    shutil.copyfile(fnfull, dest_dir + fnshort)
                                    pwdb.exc("db_file_set_renamed_name", [fnshort, fnshort], {})
                                except Exception as e:
                                    print(str(e))
                                # par2 (we exclude this strange .._sample.par2's!)
                                if p2.is_par2() and not fnfull.endswith("_sample.par2"):
                                    rarfiles = [(fn, md5) for fn, md5 in p2.md5_16khash()]
                                    p2list.append((p2, fnshort,  dest_dir + fnshort, rarfiles))
                                    pwdb.exc("db_nzb_store_p2list", [nzbname, p2list], {})
                                    pwdb.exc("db_file_set_file_type", [fnshort, "par2"], {})
                                    renamer_result_queue.put((fnshort, dest_dir + fnshort, "par2", fnshort, oldft))
                                # par2vol
                                elif p2.is_par2vol():
                                    pwdb.exc("db_file_set_file_type", [fnshort, "par2vol"], {})
                                    renamer_result_queue.put((fnshort, dest_dir + fnshort, "par2vol", fnshort, oldft))
                                    # could set # of blocks here in gpeewee
                                os.remove(fnfull)
                            else:
                                notrenamedfiles.append((fnfull, fnshort, par2lib.calc_file_md5hash_16k(fnfull)))
                # rename & move rar + remaining files
                if notrenamedfiles:
                    # da hats was
                    rename_and_move_rarandremainingfiles(p2list, notrenamedfiles, source_dir, dest_dir, pwdb,
                                                         renamer_result_queue, filewrite_lock, logger)
                isfirstrun = False
            if child_pipe.poll():
                command, _, _ = child_pipe.recv()
                if command == "pause":
                    break
        os.chdir(cwd0)
        try:
            if wd:
                inotify.rm_watch(wd)
        except Exception as e:
            logger.warning(whoami() + str(e))
        logger.debug(whoami() + "renamer paused")
    logger.info(whoami() + "exited!")
