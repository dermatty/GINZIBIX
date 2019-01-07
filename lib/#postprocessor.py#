import queue
import time
import os
import multiprocessing as mp
from .passworded_rars import get_password
from .partial_unrar import partial_unrar
import signal
import re
import glob
import shutil
import sys
from .aux import PWDBSender, mpp_is_alive
from .mplogging import setup_logger, whoami
from setproctitle import setproctitle


TERMINATED = False
PAUSED = False


def stop_wait():
    global TERMINATED, PAUSED
    while PAUSED and not TERMINATED:
        time.sleep(0.25)
    if TERMINATED:
        return True
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


def mp_join(mpp, procname, timeout=-1):
    if timeout == -1:
        timeout0 = 999999999
    else:
        timeout0 = timeout
    t0 = time.time()
    while mpp_is_alive(mpp, procname) and time.time() - t0 < timeout0:
        time.sleep(0.1)


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
                    unpack_dir, nzbdir, rename_dir, main_dir, download_dir, dirs, pw_file, event_queues_cleared, mp_loggerqueue):

    if stop_wait():
        sys.exit()

    setproctitle("gzbx." + os.path.basename(__file__))

    logger = setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")
    event_queues_cleared.clear()     # queues not cleared yet

    mpp = mpp0.copy()

    sh = SigHandler_Postprocessing(logger)
    signal.signal(signal.SIGINT, sh.sighandler_postprocessing)
    signal.signal(signal.SIGTERM, sh.sighandler_postprocessing)

    event_verifieridle = mp_events["verifier"]
    event_unrareridle = mp_events["unrarer"]

    pwdb = PWDBSender()
    post_status = pwdb.exc("db_nzb_get_poststatus", [nzbname], {})

    if post_status == 4:
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing already done/not needed!", "info"], {})
        sys.exit()

    if post_status == -4:
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing already failed!", "error"], {})
        sys.exit()

    pwdb.exc("db_msg_insert", [nzbname, "starting postprocess", "info"], {})
    logger.debug(whoami() + "starting clearing queues & pipes")

    # clear articlequeue
    while True:
        try:
            articlequeue.get_nowait()
            articlequeue.task_done()
        except (queue.Empty, EOFError, ValueError):
            break
        except Exception as e:
            logger.error(whoami() + str(e))
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            pwdb.exc("db_nzb_update_post_status", [nzbname, -1], {})
            pwdb.exc("db_msg_insert", [nzbname, "postprocessing/clearing articlequeue failed!", "error"], {})
            sys.exit()
    articlequeue.join()

    # clear resultqueue
    while True:
        try:
            resultqueue.get_nowait()
            resultqueue.task_done()
        except (queue.Empty, EOFError, ValueError):
            break
        except Exception as e:
            logger.error(whoami() + str(e))
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            pwdb.exc("db_nzb_update_post_status", [nzbname, -1], {})
            pwdb.exc("db_msg_insert", [nzbname, "postprocessing/clearing resultqueue failed!", "error"], {})
            sys.exit()
    resultqueue.join()

    # clear pipes
    try:
        for key, item in pipes.items():
            if pipes[key][0].poll():
                pipes[key][0].recv()
    except Exception as e:
        logger.error(whoami() + str(e))
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_nzb_update_post_status", [nzbname, -1], {})
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing/clearing pipes failed!", "error"], {})
        sys.exit()
    logger.debug(whoami() + "clearing queues & pipes done!")

    # join decoder
    if mpp_is_alive(mpp, "decoder"):
        try:
            while mp_work_queue.qsize() > 0:
                time.sleep(0.5)
        except Exception as e:
            logger.debug(whoami() + str(e))

    # queues & pipes cleared
    event_queues_cleared.set()
    # post status "clearing done"
    pwdb.exc("db_nzb_update_post_status", [nzbname, 1], {})
    if stop_wait():
        sys.exit()

    # join verifier
    if abs(post_status) < 2 and mpp_is_alive(mpp, "verifier"):
        logger.info(whoami() + "Waiting for par_verifier to complete")
        try:
            # kill par_verifier in deadlock
            while True:
                mp_join(mpp, "verifier", timeout=5)
                if mpp_is_alive(mpp, "verifier"):
                    # if not finished, check if idle longer than 5 sec -> deadlock!!!
                    t0 = time.time()
                    while event_verifieridle.is_set() and time.time() - t0 < 30:
                        time.sleep(0.5)
                    if time.time() - t0 >= 30:
                        logger.info(whoami() + "Verifier deadlock, killing unrarer!")
                        try:
                            os.kill(mpp["verifier"].pid, signal.SIGTERM)
                        except Exception as e:
                            logger.debug(whoami() + str(e))
                        break
                    else:
                        continue
                else:
                    break
            pwdb.exc("db_nzb_update_post_status", [nzbname, 2], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
            pwdb.exc("db_nzb_update_post_status", [nzbname, -2], {})
        mpp["verifier"] = None
        logger.debug(whoami() + "par_verifier completed/terminated!")
    if stop_wait():
        sys.exit()

    # if unrarer not running (if e.g. all files)
    ispw = pwdb.exc("db_nzb_get_ispw", [nzbname], {})
    unrarernewstarted = False
    if ispw and abs(post_status) < 3:
        get_pw_direct0 = False
        try:
            get_pw_direct0 = (cfg["OPTIONS"]["GET_PW_DIRECTLY"].lower() == "yes")
        except Exception as e:
            logger.warning(whoami() + str(e))
        if pwdb.exc("db_nzb_get_password", [nzbname], {}) == "N/A":
            logger.info("Trying to get password from file for NZB " + nzbname)
            pwdb.exc("db_msg_insert", [nzbname, "trying to get password", "info"], {})
            pw = get_password(verifiedrar_dir, pw_file, nzbname, logger, get_pw_direct=get_pw_direct0)
            if pw:
                logger.info("Found password " + pw + " for NZB " + nzbname)
                pwdb.exc("db_msg_insert", [nzbname, "found password " + pw, "info"], {})
                pwdb.exc("db_nzb_set_password", [nzbname, pw], {})
        else:
            pw = pwdb.exc("db_nzb_get_password", [nzbname], {})
        if not pw:
            logger.error("Cannot find password for NZB " + nzbname + "in postprocess, exiting ...")
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            mpp["unrarer"] = None
            # sighandler.mpp = mpp
            sys.exit()
        event_unrareridle = mp.Event()
        mpp_unrarer = mp.Process(target=partial_unrar, args=(verifiedrar_dir, unpack_dir, nzbname, mp_loggerqueue, pw, event_unrareridle, cfg, ))
        unrarernewstarted = True
        mpp_unrarer.start()
        mpp["unrarer"] = mpp_unrarer
        # sighandler.mpp = self.mpp
    # start unrarer if never started and ok verified/repaired
    elif not mpp["unrarer"] and abs(post_status) < 3:
        logger.debug(whoami() + "checking if unrarer should be started")
        try:
            verifystatus = pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
            unrarstatus = pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
        logger.debug(whoami() + "verifystatust: " + str(verifystatus) + " /unrarstatus: " + str(unrarstatus))
        if verifystatus > 0 and unrarstatus == 0:
            try:
                logger.debug(whoami() + "unrarer passiv until now, starting ...")
                event_unrareridle = mp.Event()
                mpp_unrarer = mp.Process(target=partial_unrar, args=(verifiedrar_dir, unpack_dir, nzbname, mp_loggerqueue, None, event_unrareridle, cfg, ))
                unrarernewstarted = True
                mpp_unrarer.start()
                mpp["unrarer"] = mpp_unrarer
            except Exception as e:
                logger.warning(whoami() + str(e))
    finalverifierstate = (pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
    if stop_wait():
        sys.exit()

    # join unrarer
    if mpp_is_alive(mpp, "unrarer") and abs(post_status) < 3:
        if finalverifierstate:
            logger.info(whoami() + "Waiting for unrar to complete")
            while True:
                # try to join unrarer
                if unrarernewstarted:
                    mpp["unrarer"].join(timeout=5)
                    isalive = mpp["unrarer"].is_alive()
                else:
                    mp_join(mpp, "unrarer", timeout=5)
                    isalive = mpp_is_alive(mpp, "unrarer")
                if isalive:
                    # if not finished, check if idle longer than 5 sec -> deadlock!!!
                    t0 = time.time()
                    timeout0 = 99999999 if unrarernewstarted else 120 * 2
                    while event_unrareridle.is_set() and time.time() - t0 < timeout0:
                        time.sleep(0.5)
                    if time.time() - t0 >= timeout0:
                        logger.info(whoami() + "Unrarer deadlock, killing unrarer!")
                        try:
                            os.kill(mpp["unrarer"].pid, signal.SIGTERM)
                            if unrarernewstarted:
                                mpp["unrarer"].join()
                            else:
                                mp_join(mpp, "unrarer")
                            mpp["unrarer"] = None
                        except Exception as e:
                            logger.debug(whoami() + str(e))
                        break
                    else:
                        logger.debug(whoami() + "Unrarer not idle, waiting before terminating")
                        time.sleep(0.5)
                        continue
                else:
                    break
        else:
            logger.info(whoami() + "Repair/unrar not possible, killing unrarer!")
            try:
                os.kill(mpp["unrarer"].pid, signal.SIGTERM)
                if unrarernewstarted:
                    mpp["unrarer"].join()
                else:
                    mp_join(mpp, "unrarer")
                mpp["unrarer"] = None
            except Exception as e:
                logger.debug(whoami() + str(e))
        mpp["unrarer"] = None
        # sighandler.mpp = self.mpp
        logger.debug(whoami() + "unrarer completed/terminated!")
    if stop_wait():
        sys.exit()

    # get status
    finalverifierstate = (pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
    finalnonrarstate = pwdb.exc("db_allnonrarfiles_getstate", [nzbname], {})
    finalrarstate = (pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) in [0, 2])
    if finalrarstate:
        pwdb.exc("db_nzb_update_post_status", [nzbname, 3], {})
    else:
        pwdb.exc("db_nzb_update_post_status", [nzbname, -3], {})
    logger.info(whoami() + "Finalverifierstate: " + str(finalverifierstate) + " / Finalrarstate: " + str(finalrarstate) + " / Finalnonrarstate: "
                + str(finalnonrarstate))
    if finalrarstate and finalnonrarstate and finalverifierstate:
        pwdb.exc("db_msg_insert", [nzbname, "unrar/par-repair ok!", "success"], {})
    else:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, "unrar/par-repair failed!", "error"], {})
        logger.info("postprocess of NZB " + nzbname + " failed!")
        pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
        sys.exit()
    if stop_wait():
        sys.exit()

    if not abs(post_status) < 4:
        sys.exit()

    # copy to complete
    pwdb.exc("db_msg_insert", [nzbname, "copying & cleaning directories", "info"], {})
    complete_dir = make_complete_dir(dirs, nzbdir, logger)
    if not complete_dir:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
        logger.info("Cannot create complete_dir for " + nzbname + ", exiting ...")
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing failed!", "error"], {})
        pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
        sys.exit()
    # move all non-rar/par2/par2vol files from renamed to complete
    for f00 in glob.glob(rename_dir + "*"):
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
                    pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
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
                pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot remove rar/par2 file!")
        else:
            try:
                shutil.move(f00, complete_dir)
                logger.debug(whoami() + "moved non-rar/non-par2 file " + f0 + " to complete")
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot move non-rar/non-par2 file " + f00 + "!")
    # remove download_dir
    try:
        shutil.rmtree(download_dir)
    except Exception as e:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
        logger.warning(whoami() + str(e) + ": cannot remove download_dir!")
    # move content of unpack dir to complete
    logger.debug(whoami() + "moving unpack_dir to complete: " + unpack_dir)
    for f00 in glob.glob(unpack_dir + "*"):
        logger.debug(whoami() + "u1npack_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
        d0 = f00.split("/")[-1]
        logger.debug(whoami() + "Does " + complete_dir + d0 + " already exist?")
        if os.path.isfile(complete_dir + d0):
            try:
                logger.debug(whoami() + complete_dir + d0 + " already exists, deleting!")
                os.remove(complete_dir + d0)
            except Exception as e:
                logger.debug(whoami() + f00 + " already exists but cannot delete")
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
                break
        else:
            logger.debug(whoami() + complete_dir + d0 + " does not exist!")

        if not os.path.isdir(f00):
            try:
                shutil.move(f00, complete_dir)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
                logger.warning(str(e) + ": cannot move unrared file to complete dir!")
        else:
            if os.path.isdir(complete_dir + d0):
                try:
                    shutil.rmtree(complete_dir + d0)
                except Exception as e:
                    pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
                    logger.warning(str(e) + ": cannot remove unrared dir in complete!")
            try:
                shutil.copytree(f00, complete_dir + d0)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
                logger.warning(str(e) + ": cannot move non-rar/non-par2 file!")
    # remove unpack_dir
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
        try:
            shutil.rmtree(unpack_dir)
            shutil.rmtree(verifiedrar_dir)
        except Exception as e:
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
            logger.warning(str(e) + ": cannot remove unpack_dir / verifiedrar_dir")
    # remove incomplete_dir
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
        try:
            shutil.rmtree(main_dir)
        except Exception as e:
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
            logger.warning(str(e) + ": cannot remove incomplete_dir!")
    # finalize
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) == -4:
        logger.info("Copy/Move of NZB " + nzbname + " failed!")
        pwdb.exc("db_nzb_update_post_status", [nzbname, -4], {})
        sys.exit()
    else:
        logger.info("Copy/Move of NZB " + nzbname + " success!")
        pwdb.exc("db_nzb_update_status", [nzbname, 4], {})
        pwdb.exc("db_nzb_update_post_status", [nzbname, 4], {})
        logger.info("Postprocess of NZB " + nzbname + " ok!")
        sys.exit()
