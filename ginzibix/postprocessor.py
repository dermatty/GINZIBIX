import queue
import time
import os
import multiprocessing as mp
import signal
import re
import glob
import shutil
import sys
from .mplogging import setup_logger, whoami
from setproctitle import setproctitle

from ginzibix.mplogging import whoami
from ginzibix import mplogging, partial_unrar, par_verifier, passworded_rars
from ginzibix import PWDBSender, make_dirs, mpp_is_alive, mpp_join, GUI_Poller, get_cut_nzbname, get_cut_msg, get_bg_color, get_status_name_and_color,\
    clear_postproc_dirs, get_server_config, get_configured_servers, get_config_for_server, get_free_server_cfg, is_port_in_use, do_mpconnections,\
    kill_mpp


TERMINATED = False
PAUSED = False


def stop_wait(nzbname, dirs, pwdb):
    global TERMINATED, PAUSED
    while PAUSED and not TERMINATED:
        time.sleep(0.25)
    if TERMINATED:
        pwdb.exc("db_nzb_undo_postprocess", [nzbname], {})
        clear_postproc_dirs(nzbname, dirs)
        sys.exit()
    return False


def postproc_pause():
    global PAUSED
    PAUSED = True


def postproc_resume():
    global PAUSED
    PAUSED = False


class SigHandler_Postprocessing:
    def __init__(self, logger):
        self.logger = logger

    def sighandler_postprocessing(self, a, b):
        global TERMINATED
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


def make_complete_dir(dirs, nzbdir, logger):
    complete_dir = dirs["complete"] + nzbdir
    try:
        if not os.path.isdir(dirs["complete"]):
            os.mkdir(dirs["complete"])
    except Exception as e:
        logger.error(str(e) + " in creating complete ...")
        return False
    if os.path.isdir(complete_dir):
        try:
            shutil.rmtree(complete_dir)
        except Exception as e:
            logger.error(str(e) + " in deleting complete_dir ...")
            return False
    try:
        if not os.path.isdir(complete_dir):
            os.mkdir(complete_dir)
        time.sleep(1)
        return complete_dir
    except Exception as e:
        logger.error(str(e) + " in creating dirs ...")
        return False


def postprocess_nzb(nzbname, articlequeue, resultqueue, mp_work_queue, pipes, mpp0, mp_events, cfg, verifiedrar_dir,
                    unpack_dir, nzbdir, rename_dir, main_dir, download_dir, dirs, pw_file, mp_loggerqueue):

    setproctitle("gzbx." + os.path.basename(__file__))
    pwdb = PWDBSender()

    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) in [4, -4]:
        sys.exit()

    logger = setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")

    nzbdirname = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
    incompletedir = dirs["incomplete"] + nzbdirname
    if not os.path.isdir(incompletedir):
        logger.error(whoami() + "no incomplete_dir, aborting postprocessing")
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        sys.exit()

    mpp = mpp0.copy()
    sh = SigHandler_Postprocessing(logger)
    signal.signal(signal.SIGINT, sh.sighandler_postprocessing)
    signal.signal(signal.SIGTERM, sh.sighandler_postprocessing)

    stop_wait(nzbname, dirs, pwdb)

    event_verifieridle = mp_events["verifier"]
    event_unrareridle = mp_events["unrarer"]

    pwdb.exc("db_msg_insert", [nzbname, "starting postprocess", "info"], {})
    logger.debug(whoami() + "starting clearing queues & pipes")

    # clear pipes
    do_mpconnections(pipes, "clearqueues", None)
    try:
        for key, item in pipes.items():
            if pipes[key][0].poll():
                pipes[key][0].recv()
    except Exception as e:
        logger.error(whoami() + str(e))
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing/clearing pipes failed!", "error"], {})
        sys.exit()
    logger.debug(whoami() + "clearing queues & pipes done!")

    # join decoder
    if mpp_is_alive(mpp, "decoder"):
        t0 = time.time()
        timeout_reached = False
        try:
            while mp_work_queue.qsize() > 0:
                if time.time() - t0 > 60 * 3:
                    timeout_reached = True
                    break
                time.sleep(0.5)
        except Exception as e:
            logger.debug(whoami() + str(e))
            timeout_reached = True
        if timeout_reached:
            logger.warning(whoami() + "Timeout reached/error in joining decoder, terminating decoder ...")
            kill_mpp(mpp, "decoder")
            logger.info(whoami() + "decoder terminated!")
            try:
                while not mp_work_queue.empty():
                    mp_work_queue.get_no_wait()
            except Exception:
                pass

    stop_wait(nzbname, dirs, pwdb)

    # --- PAR_VERIFIER ---
    verifystatus = pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
    all_rars_are_verified, _ = pwdb.exc("db_only_verified_rars", [nzbname], {})
    renamed_rar_files = pwdb.exc("get_all_renamed_rar_files", [nzbname], {})
    # verifier not running, status = 0 --> start & wait
    if not mpp_is_alive(mpp, "verifier") and verifystatus == 0 and not all_rars_are_verified and renamed_rar_files:
        logger.info(whoami() + "there are files to check by par_verifier, starting par_verifier ...")
        p2 = pwdb.exc("get_renamed_p2", [rename_dir, nzbname], {})
        if p2:
            pvmode = "verify"
        else:
            pvmode = "copy"
        mpp_verifier = mp.Process(target=par_verifier.par_verifier, args=(pipes["verifier"][1], rename_dir, verifiedrar_dir,
                                                             main_dir, mp_loggerqueue, nzbname, pvmode, event_verifieridle,
                                                             cfg, ))
        mpp_verifier.start()
        mpp["verifier"] = mpp_verifier
        time.sleep(1)

    stop_wait(nzbname, dirs, pwdb)

    verifystatus = pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
    # verifier is running, status == 1 (running) or -2 -> wait/join
    if mpp_is_alive(mpp, "verifier") and verifystatus in [1, -2]:
        logger.info(whoami() + "Waiting for par_verifier to complete")
        try:
            # kill par_verifier in deadlock
            while True:
                mpp_join(mpp, "verifier", timeout=5)
                if mpp_is_alive(mpp, "verifier"):
                    # if not finished, check if idle longer than 5 sec -> deadlock!!!
                    t0 = time.time()
                    while event_verifieridle.is_set() and time.time() - t0 < 30:
                        time.sleep(0.5)
                    if time.time() - t0 >= 30:
                        logger.info(whoami() + "Verifier deadlock, killing verifier!")
                        kill_mpp(mpp, "verifier")
                        break
                    else:
                        continue
                else:
                    break
        except Exception as e:
            logger.warning(whoami() + str(e))
        mpp_join(mpp, "verifier")
        mpp["verifier"] = None
        logger.debug(whoami() + "par_verifier completed/terminated!")
    # if verifier is running but wrong status, or correct status but not running, exit
    elif mpp_is_alive(mpp, "verifier") or verifystatus == 1:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, "par_verifier status inconsistency", "error"], {})
        logger.debug(whoami() + "something is wrong with par_verifier, exiting postprocessor")
        logger.info(whoami() + "postprocess of NZB " + nzbname + " failed!")
        if mpp_is_alive(mpp, "verifier"):
            logger.debug(whoami() + "terminating par_verifier")
            kill_mpp(mpp, "verifier")
            logger.info(whoami() + "verifier terminated!")
        sys.exit()

    stop_wait(nzbname, dirs, pwdb)

    # ---  UNRARER ---
    # already checked if pw protected?
    is_pw_checked = pwdb.exc("db_nzb_get_ispw_checked", [nzbname], {})
    if not is_pw_checked:
        logger.debug(whoami() + "testing if pw protected")
        ispw = passworded_rars.is_rar_password_protected(verifiedrar_dir, logger)
        pwdb.exc("db_nzb_set_ispw_checked", [nzbname, True], {})
        if ispw == 0 or ispw == -2 or ispw == -3:
            logger.warning(whoami() + "cannot test rar if pw protected, something is wrong: " + str(ispw) + ", exiting ...")
            pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed due to pw test not possible", "error"], {})
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            sys.exit()
        elif ispw == 1:
            pwdb.exc("db_nzb_set_ispw", [nzbname, True], {})
            pwdb.exc("db_msg_insert", [nzbname, "rar archive is password protected", "warning"], {})
            logger.info(whoami() + "rar archive is pw protected")
        elif ispw == -1:
            pwdb.exc("db_nzb_set_ispw", [nzbname, False], {})
            pwdb.exc("db_msg_insert", [nzbname, "rar archive is NOT password protected", "info"], {})
            logger.info(whoami() + "rar archive is NOT pw protected")
    ispw = pwdb.exc("db_nzb_get_ispw", [nzbname], {})
    unrarernewstarted = False
    if ispw:
        get_pw_direct0 = False
        try:
            get_pw_direct0 = (cfg["OPTIONS"]["GET_PW_DIRECTLY"].lower() == "yes")
        except Exception as e:
            logger.warning(whoami() + str(e))
        if pwdb.exc("db_nzb_get_password", [nzbname], {}) == "N/A":
            logger.info(whoami() + "Trying to get password from file for NZB " + nzbname)
            pwdb.exc("db_msg_insert", [nzbname, "trying to get password", "info"], {})
            pw = passworded_rars.get_password(verifiedrar_dir, pw_file, nzbname, logger, get_pw_direct=get_pw_direct0)
            if pw:
                logger.info(whoami() + "Found password " + pw + " for NZB " + nzbname)
                pwdb.exc("db_msg_insert", [nzbname, "found password " + pw, "info"], {})
                pwdb.exc("db_nzb_set_password", [nzbname, pw], {})
        else:
            pw = pwdb.exc("db_nzb_get_password", [nzbname], {})
        if not pw:
            pwdb.exc("db_msg_insert", [nzbname, "Provided password was not correct / no password found in PW file! ", "error"], {})
            logger.error(whoami() + "Cannot find password for NZB " + nzbname + "in postprocess, exiting ...")
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            mpp["unrarer"] = None
            # sighandler.mpp = mpp
            sys.exit()
        event_unrareridle = mp.Event()
        # passing p2list to unrarer
        mpp_unrarer = mp.Process(target=partial_unrar.partial_unrar, args=(verifiedrar_dir, unpack_dir, nzbname, mp_loggerqueue, pw, event_unrareridle, cfg, ))
        unrarernewstarted = True
        mpp_unrarer.start()
        mpp["unrarer"] = mpp_unrarer
        # sighandler.mpp = self.mpp
    # start unrarer if never started and ok verified/repaired
    elif not mpp["unrarer"]:
        logger.debug(whoami() + "checking if unrarer should be started")
        try:
            verifystatus = pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
            unrarstatus = pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
        logger.debug(whoami() + "verifystatus: " + str(verifystatus) + " / unrarstatus: " + str(unrarstatus))
        if (verifystatus > 0 or verifystatus == -2) and unrarstatus == 0:
            try:
                logger.debug(whoami() + "unrarer passiv until now, starting ...")
                event_unrareridle = mp.Event()
                # passing p2list to unrarer
                mpp_unrarer = mp.Process(target=partial_unrar.partial_unrar, args=(verifiedrar_dir, unpack_dir, nzbname, mp_loggerqueue, None,
                                                                                   event_unrareridle, cfg, ))
                unrarernewstarted = True
                mpp_unrarer.start()
                mpp["unrarer"] = mpp_unrarer
            except Exception as e:
                logger.warning(whoami() + str(e))
    finalverifierstate = (pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])

    stop_wait(nzbname, dirs, pwdb)

    # join unrarer
    if mpp_is_alive(mpp, "unrarer"):
        if finalverifierstate:
            logger.info(whoami() + "Waiting for unrar to complete")
            while True:
                # try to join unrarer
                mpp_join(mpp, "unrarer", timeout=5)
                isalive = mpp_is_alive(mpp, "unrarer")
                if isalive:
                    # if not finished, check if idle longer than 5 sec -> deadlock!!!
                    t0 = time.time()
                    timeout0 = 99999999 if unrarernewstarted else 120 * 2
                    while event_unrareridle.is_set() and time.time() - t0 < timeout0:
                        time.sleep(0.5)
                    if time.time() - t0 >= timeout0:
                        logger.info(whoami() + "Unrarer deadlock, killing unrarer!")
                        kill_mpp(mpp, "unrarer")
                        break
                    else:
                        logger.debug(whoami() + "Unrarer not idle, waiting before terminating")
                        stop_wait(nzbname, dirs, pwdb)
                        time.sleep(0.5)
                        continue
                else:
                    break
        else:
            logger.info(whoami() + "Repair/unrar not possible, killing unrarer!")
            kill_mpp(mpp, "unrarer")
        logger.debug(whoami() + "unrarer completed/terminated!")

    stop_wait(nzbname, dirs, pwdb)

    # get status
    finalverifierstate = (pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
    finalnonrarstate = pwdb.exc("db_allnonrarfiles_getstate", [nzbname], {})
    finalrarstate = (pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) in [0, 2])
    logger.info(whoami() + "Finalverifierstate: " + str(finalverifierstate) + " / Finalrarstate: " + str(finalrarstate) + " / Finalnonrarstate: "
                + str(finalnonrarstate))
    if finalrarstate and finalnonrarstate and finalverifierstate:
        pwdb.exc("db_msg_insert", [nzbname, "unrar/par-repair ok!", "success"], {})
        logger.info(whoami() + "unrar/par-repair of NZB " + nzbname + " success!")
    else:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, "unrar/par-repair failed!", "error"], {})
        logger.info(whoami() + "postprocess of NZB " + nzbname + " failed!")
        sys.exit()

    stop_wait(nzbname, dirs, pwdb)

    # copy to complete
    logger.info(whoami() + "starting copy-to-complete")
    pwdb.exc("db_msg_insert", [nzbname, "copying & cleaning directories", "info"], {})
    complete_dir = make_complete_dir(dirs, nzbdir, logger)
    if not complete_dir:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
        logger.info("Cannot create complete_dir for " + nzbname + ", exiting ...")
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
        sys.exit()
    # move all non-rar/par2/par2vol files from renamed to complete
    for f00 in glob.glob(rename_dir + "*") + glob.glob(rename_dir + ".*"):
        logger.debug(whoami() + "renamed_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
        if os.path.isdir(f00):
            logger.debug(f00 + "is a directory, skipping")
            continue
        f0 = f00.split("/")[-1]
        file0type = pwdb.exc("db_file_getftype_renamed", [f0], {})
        logger.debug(whoami() + "Moving/deleting " + f0)
        if not file0type:
            gg = re.search(r"[0-9]+[.]rar[.]+[0-9]", f0, flags=re.IGNORECASE)
            if gg:
                try:
                    os.remove(f00)
                    logger.debug(whoami() + "Removed rar.x file " + f0)
                except Exception as e:
                    pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    logger.warning(whoami() + str(e) + ": cannot remove corrupt rar file!")
            else:    # if unknown file (not in db) move to complete anyway
                try:
                    shutil.move(f00, complete_dir)
                    logger.debug(whoami() + "moved " + f00 + " to " + complete_dir)
                except Exception as e:
                    logger.warning(whoami() + str(e) + ": cannot move unknown file to complete!")
            continue
        if file0type in ["rar", "par2", "par2vol"]:
            try:
                os.remove(f00)
                logger.debug(whoami() + "removed rar/par2 file " + f0)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot remove rar/par2 file!")
        else:
            try:
                shutil.move(f00, complete_dir)
                logger.debug(whoami() + "moved non-rar/non-par2 file " + f0 + " to complete")
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot move non-rar/non-par2 file " + f00 + "!")

    stop_wait(nzbname, dirs, pwdb)

    # remove download_dir
    try:
        shutil.rmtree(download_dir)
    except Exception as e:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        logger.warning(whoami() + str(e) + ": cannot remove download_dir!")
    # move content of unpack dir to complete
    logger.debug(whoami() + "moving unpack_dir to complete: " + unpack_dir)
    for f00 in glob.glob(unpack_dir + "*") + glob.glob(unpack_dir + ".*"):
        logger.debug(whoami() + "unpack_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
        d0 = f00.split("/")[-1]
        logger.debug(whoami() + "Does " + complete_dir + d0 + " already exist?")
        if os.path.isfile(complete_dir + d0):
            try:
                logger.debug(whoami() + complete_dir + d0 + " already exists, deleting!")
                os.remove(complete_dir + d0)
            except Exception:
                logger.debug(whoami() + f00 + " already exists but cannot delete")
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                break
        else:
            logger.debug(whoami() + complete_dir + d0 + " does not exist!")

        if not os.path.isdir(f00):
            try:
                shutil.move(f00, complete_dir)
                logger.debug(whoami() + ": moved " + f00 + " to " + complete_dir)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot move unrared file to complete dir!")
        else:
            if os.path.isdir(complete_dir + d0):
                try:
                    shutil.rmtree(complete_dir + d0)
                    logger.debug(whoami() + ": removed tree " + complete_dir + d0)
                except Exception as e:
                    pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    logger.warning(whoami() + str(e) + ": cannot remove unrared dir in complete!")
            try:
                logger.debug(whoami() + "copying tree " + f00 + " to " + complete_dir + d0)
                shutil.copytree(f00, complete_dir + d0)
                logger.debug(whoami() + "copied tree " + f00 + " to " + complete_dir + d0)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot move non-rar/non-par2 file!")
    # remove unpack_dir
    logger.debug(whoami() + ": removing unpacl_dir, verified_rardir and incomplete_dir")
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
        try:
            shutil.rmtree(unpack_dir)
            shutil.rmtree(verifiedrar_dir)
            logger.debug(whoami() + ": removed " + unpack_dir + verifiedrar_dir)
        except Exception as e:
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            logger.warning(whoami() + str(e) + ": cannot remove unpack_dir / verifiedrar_dir")
    # remove incomplete_dir
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
        try:
            shutil.rmtree(main_dir)
            logger.debug(whoami() + ": removed " + main_dir)
        except Exception as e:
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            logger.warning(whoami() + str(e) + ": cannot remove incomplete_dir!")
    # finalize
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) == -4:
        logger.info(whoami() + "Copy/Move of NZB " + nzbname + " failed!")
    else:
        logger.info(whoami() + "Copy/Move of NZB " + nzbname + " success!")
        pwdb.exc("db_nzb_update_status", [nzbname, 4], {})
        logger.info(whoami() + "Postprocess of NZB " + nzbname + " success!")
    sys.exit()
