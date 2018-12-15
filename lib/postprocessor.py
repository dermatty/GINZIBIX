import inspect
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
from .aux import PWDBSender


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


TERMINATED = False
IS_IDLE = False


class SigHandler_Postprocessing:
    def __init__(self, logger):
        self.logger = logger

    def sighandler_postprocessing(self, a, b):
        global TERMINATED
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


def postprocess_nzb(self, nzbname, articlequeue, resultqueue, mp_work_queue, pipes, mpp, mp_events, cfg, verifiedrar_dir,
                    unpack_dir, pw_file, logger):
    logger.debug(whoami() + "starting ...")
    sh = SigHandler_Postprocessing(logger)
    signal.signal(signal.SIGINT, sh.sighandler_postprocessing)
    signal.signal(signal.SIGTERM, sh.sighandler_postprocessing)

    event_verifieridle = mp_events["verifier"]
    event_unrareridle = mp_events["unrarer"]

    pwdb = PWDBSender()

    pwdb.exc("db_msg_insert", [nzbname, "starting postprocess", "info"], {})
    self.logger.debug(whoami() + "starting clearing queues & pipes")

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
            pwdb.exc("db_msg_insert", [nzbname, nzbname + ": postprocessing/clearing articlequeue failed!", "error"], {})
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
            pwdb.exc("db_msg_insert", [nzbname, nzbname + ": postprocessing/clearing resultqueue failed!", "error"], {})
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
        pwdb.exc("db_msg_insert", [nzbname, nzbname + ": postprocessing/clearing pipes failed!", "error"], {})
        sys.exit()
    self.logger.debug(whoami() + "clearing queues & pipes done!")

    # join decoder
    if mpp["decoder"]:
        if mpp["decoder"].is_alive():
            try:
                while mp_work_queue.qsize() > 0:
                    time.sleep(0.5)
            except Exception as e:
                logger.debug(whoami() + str(e))

    # join verifier
    if self.mpp["verifier"]:
        logger.info(whoami() + "Waiting for par_verifier to complete")
        try:
            # kill par_verifier in deadlock
            while True:
                mpp["verifier"].join(timeout=2)
                if mpp["verifier"].is_alive():
                    # if not finished, check if idle longer than 5 sec -> deadlock!!!
                    t0 = time.time()
                    while event_verifieridle.is_set() and time.time() - t0 < 5:
                        time.sleep(0.5)
                    if time.time() - t0 >= 5:
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
        except Exception as e:
            logger.warning(str(e))
        self.mpp["verifier"] = None
        logger.debug(whoami() + "par_verifier completed/terminated!")

    # if unrarer not running (if e.g. all files)
    ispw = pwdb.exc("db_nzb_get_ispw", [nzbname], {})
    if ispw:
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
            # sighandler.mpp = self.mpp
            sys.exit()
        mpp_unrarer = mp.Process(target=partial_unrar, args=(verifiedrar_dir, unpack_dir, nzbname, logger, pw, cfg, ))
        mpp_unrarer.start()
        mpp["unrarer"] = self.mpp_unrarer
        # sighandler.mpp = self.mpp
    # start unrarer if never started and ok verified/repaired
    elif not mpp["unrarer"]:
        try:
            verifystatus = pwdb.exc("db_nzb_get_verifystatus", [nzbname], {})
            unrarstatus = pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
        if verifystatus > 0 and unrarstatus == 0:
            try:
                logger.debug(whoami() + "unrarer passiv until now, starting ...")
                mpp_unrarer = mp.Process(target=partial_unrar, args=(verifiedrar_dir, unpack_dir, nzbname, logger, None, cfg, ))
                mpp_unrarer.start()
            except Exception as e:
                logger.warning(whoami() + str(e))
    finalverifierstate = (pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
    # join unrarer
    if self.mpp["unrarer"]:
        if finalverifierstate:
            logger.info("Waiting for unrar to complete")
            while True:
                # try to join unrarer
                mpp["unrarer"].join(timeout=2)
                if mpp["unrarer"].is_alive():
                    # if not finished, check if idle longer than 5 sec -> deadlock!!!
                    t0 = time.time()
                    while event_unrareridle.is_set() and time.time() - t0 < 5:
                        time.sleep(0.5)
                    if time.time() - t0 >= 5:
                        logger.info(whoami() + "Unrarer deadlock, killing unrarer!")
                        try:
                            os.kill(self.mpp["unrarer"].pid, signal.SIGTERM)
                        except Exception as e:
                            logger.debug(whoami() + str(e))
                        break
                    else:
                        logger.debug(whoami() + "Unrarer not idle, waiting before terminating")
                        continue
                else:
                    break
        else:
            logger.info("Repair/unrar not possible, killing unrarer!")
            try:
                os.kill(self.mpp["unrarer"].pid, signal.SIGTERM)
            except Exception as e:
                logger.debug(whoami() + str(e))
        self.mpp["unrarer"] = None
        self.sighandler.mpp = self.mpp
        logger.debug(whoami() + "unrarer completed/terminated!")
    # get status
    finalverifierstate = (pwdb.exc("db_nzb_get_verifystatus", [nzbname], {}) in [0, 2])
    finalnonrarstate = pwdb.exc("db_allnonrarfiles_getstate", [nzbname], {})
    finalrarstate = (pwdb.exc("db_nzb_get_unrarstatus", [nzbname], {}) in [0, 2])
    logger.info("Finalrarstate: " + str(finalrarstate) + " / Finalnonrarstate: " + str(finalnonrarstate))
    if finalrarstate and finalnonrarstate and finalverifierstate:
        pwdb.exc("db_msg_insert", [nzbname, nzbname + ": postprocessing ok!", "success"], {})
    else:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, nzbname + ": postprocessing failed!", "error"], {})
        logger.info("postprocess of NZB " + nzbname + " failed!")
        return -1
    # copy to complete
    res0 = self.make_complete_dir()
    if not res0:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        pwdb.exc("db_msg_insert", [nzbname, nzbname + ": postprocessing failed!", "error"], {})
        logger.info("Cannot create complete_dir for " + nzbname + ", exiting ...")
        pwdb.exc("db_msg_insert", [nzbname, nzbname + ":postprocessing failed!", "error"], {})
        return -1
    # move all non-rar/par2/par2vol files from renamed to complete
    for f00 in glob.glob(self.rename_dir + "*"):
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
                    shutil.move(f00, self.complete_dir)
                    logger.debug(whoami() + "moved " + f00 + " to " + self.complete_dir)
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
                shutil.move(f00, self.complete_dir)
                logger.debug(whoami() + "moved non-rar/non-par2 file " + f0 + " to complete")
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(whoami() + str(e) + ": cannot move non-rar/non-par2 file " + f00 + "!")
    # remove download_dir
    try:
        shutil.rmtree(self.download_dir)
    except Exception as e:
        pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
        logger.warning(whoami() + str(e) + ": cannot remove download_dir!")
    # move content of unpack dir to complete
    logger.debug(whoami() + "moving unpack_dir to complete: " + self.unpack_dir)
    for f00 in glob.glob(self.unpack_dir + "*"):
        logger.debug(whoami() + "u1npack_dir: checking " + f00 + " / " + str(os.path.isdir(f00)))
        d0 = f00.split("/")[-1]
        logger.debug(whoami() + "Does " + self.complete_dir + d0 + " already exist?")
        if os.path.isfile(self.complete_dir + d0):
            try:
                logger.debug(whoami() + self.complete_dir + d0 + " already exists, deleting!")
                os.remove(self.complete_dir + d0)
            except Exception as e:
                logger.debug(whoami() + f00 + " already exists but cannot delete")
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                break
        else:
            logger.debug(whoami() + self.complete_dir + d0 + " does not exist!")

        if not os.path.isdir(f00):
            try:
                shutil.move(f00, self.complete_dir)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(str(e) + ": cannot move unrared file to complete dir!")
        else:
            if os.path.isdir(self.complete_dir + d0):
                try:
                    shutil.rmtree(self.complete_dir + d0)
                except Exception as e:
                    pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                    logger.warning(str(e) + ": cannot remove unrared dir in complete!")
            try:
                shutil.copytree(f00, self.complete_dir + d0)
            except Exception as e:
                pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
                logger.warning(str(e) + ": cannot move non-rar/non-par2 file!")
    # remove unpack_dir
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
        try:
            shutil.rmtree(self.unpack_dir)
            shutil.rmtree(self.verifiedrar_dir)
        except Exception as e:
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            logger.warning(str(e) + ": cannot remove unpack_dir / verifiedrar_dir")
    # remove incomplete_dir
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) != -4:
        try:
            shutil.rmtree(self.main_dir)
        except Exception as e:
            pwdb.exc("db_nzb_update_status", [nzbname, -4], {})
            logger.warning(str(e) + ": cannot remove incomplete_dir!")
    # finalize
    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) == -4:
        logger.info("Copy/Move of NZB " + nzbname + " failed!")
        pwdb.exc("db_msg_insert"[nzbname, "postprocessing failed!", "error"], {})
        self.guiconnector.set_data(downloaddata, self.ct.threads, self.ct.servers.server_config, "failed", self.serverconfig())
        return -1
    else:
        logger.info("Copy/Move of NZB " + nzbname + " success!")
        self.guiconnector.set_data(downloaddata, self.ct.threads, self.ct.servers.server_config, "success", self.serverconfig())
        pwdb.exc("db_nzb_update_status", [nzbname, 4], {})
        pwdb.exc("db_msg_insert", [nzbname, "postprocessing success!", "success"], {})
        logger.info("Postprocess of NZB " + nzbname + " ok!")
        return 1
