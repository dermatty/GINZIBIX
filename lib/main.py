#!/home/stephan/.virtualenvs/nntp/bin/python

import zmq
import sys
import time
import os
import datetime
import queue
import signal
import multiprocessing as mp
import re
import threading
import shutil
from .renamer import renamer
from .nzb_parser import ParseNZB
from .connections import ConnectionThreads
from .aux import PWDBSender, mpp_is_alive, clear_postproc_dirs, get_configured_servers, do_mpconnections
from .postprocessor import postprocess_nzb, postproc_pause, postproc_resume
from .mplogging import setup_logger, whoami
from .downloader import Downloader
from .mpconnections import mpconnector
from setproctitle import setproctitle
from collections import deque
import pandas as pd
import pickle

GB_DIVISOR = (1024 * 1024 * 1024)
CONNECTIONS_AS_MP = False


class SigHandler_Main:

    def __init__(self, event_stopped, logger):
        self.logger = logger
        self.event_stopped = event_stopped

    def sighandler(self, a, b):
        self.event_stopped.set()
        self.logger.debug(whoami() + "set event_stopped = True")


# serverstats["eweka"] = pd.Series
def update_server_ts(server_ts, ct, pipes):

    now0 = datetime.datetime.now().replace(microsecond=0)
    if server_ts:
        mint = min([(now0 - server_ts[server]["sec"].index[-1]).total_seconds() for server in server_ts])
    else:
        mint = 1
    if mint < 1:
        return

    hour_in_sec = 3600
    day_in_sec = hour_in_sec * 24
    months_in_sec = day_in_sec * 90

    if CONNECTIONS_AS_MP:
        current_stats = do_mpconnections(pipes, "get_downloaded_per_server", None)
    else:
        current_stats = ct.get_downloaded_per_server()

    for server, bdl in current_stats.items():
        bdl = bdl / (1024 * 1024)   # in MB
        try:
            assert(server_ts[server]["sec"].index[-1])
        except Exception:
            server_ts[server] = {}
            now0_minus1 = now0 - datetime.timedelta(seconds=1)
            server_ts[server]["sec"] = pd.Series(bdl, index=pd.date_range(now0_minus1, periods=2, freq='S'))
            server_ts[server]["minute"] = pd.Series(bdl, index=pd.date_range(now0_minus1, periods=2, freq='min'))
            server_ts[server]["hour"] = pd.Series(bdl, index=pd.date_range(now0_minus1, periods=2, freq='H'))
            server_ts[server]["day"] = pd.Series(bdl, index=pd.date_range(now0_minus1, periods=2, freq='D'))

        server_ts[server]["sec"][now0] = bdl

        df_min_add = server_ts[server]["sec"].resample("1T").mean()[-1]
        server_ts[server]["minute"][-1] = df_min_add

        df_h_add = server_ts[server]["minute"].resample("1H").mean()[-1]
        server_ts[server]["hour"][-1] = df_h_add

        df_d_add = server_ts[server]["hour"].resample("1D").mean()[-1]
        server_ts[server]["day"][-1] = df_d_add

        # limit seconds to 60
        while (now0 - server_ts[server]["sec"].index[0]).total_seconds() > 60:
            server_ts[server]["sec"] = server_ts[server]["sec"].drop(server_ts[server]["sec"].index[0])

        # every 60 sec: minutes entry, max 60 minutes
        if (now0 - server_ts[server]["minute"].index[-1]).total_seconds() >= 60:
            server_ts[server]["minute"][now0] = bdl
            while (now0 - server_ts[server]["minute"].index[0]).total_seconds() > hour_in_sec:
                server_ts[server]["minute"] = server_ts[server]["minute"].drop(server_ts[server]["minute"].index[0])

        # every 60 minutes: hour entry, max 24 hours
        if (now0 - server_ts[server]["hour"].index[-1]).total_seconds() >= hour_in_sec:
            server_ts[server]["hour"][now0] = bdl
            while (now0 - server_ts[server]["hour"].index[0]).total_seconds() > day_in_sec:
                server_ts[server]["hour"] = server_ts[server]["hour"].drop(server_ts[server]["hour"].index[0])

        # every 24 hours: day entry, max 90 days
        if (now0 - server_ts[server]["day"].index[-1]).total_seconds() >= day_in_sec:
            server_ts[server]["day"][now0] = bdl
            while (now0 - server_ts[server]["day"].index[0]).total_seconds() > months_in_sec:
                server_ts[server]["day"] = server_ts[server]["day"].drop(server_ts[server]["day"].index[0])
    return


def make_allfilelist_wait(pwdb, dirs, logger, timeout0):
    # immediatley get allfileslist
    try:
        nzbname = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
        if nzbname:
            logger.debug(whoami() + "no timeout, got nzb " + nzbname + " immediately!")
            return nzbname
        elif timeout0 and timeout0 <= -1:
            return None
    except Exception as e:
        logger.warning(whoami() + str(e))
        return None
    # setup inotify
    logger.debug(whoami() + "waiting for new nzb with timeout=" + str(timeout0))
    t0 = time.time()
    if not timeout0:
        delay0 = 5
    else:
        delay0 = 1
    while True:
        try:
            nzbname = pwdb.exc("make_allfilelist", [dirs["incomplete"], dirs["nzb"]], {})
        except Exception as e:
            logger.warning(whoami() + str(e))
        if nzbname:
            logger.debug(whoami() + "new nzb found in db, queuing ...")
            return nzbname
        if timeout0:
            if time.time() - t0 > timeout0 / 1000:
                break
        time.sleep(delay0)
    return None


def clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False, onlyarticlequeue=True):
    # 1. join & clear all queues
    if CONNECTIONS_AS_MP:
        do_mpconnections(pipes, "clear_articlequeue", None)
        if onlyarticlequeue:
            logger.info(whoami() + "articlequeue cleared!")
        else:
            do_mpconnections(pipes, "clear_resultqueue", None)
            logger.info(whoami() + "articlequeue & resultqueue cleared!")
    elif dl:
        dl.clear_queues_and_pipes(onlyarticlequeue)
        if onlyarticlequeue:
            logger.info(whoami() + "articlequeue cleared!")
        else:
            logger.info(whoami() + "articlequeue & resultqueue cleared!")
    # 2. stop article_decoder
    try:
        if mpp_is_alive(mpp, "decoder"):
            mpid = mpp["decoder"].pid
            logger.debug(whoami() + "terminating decoder")
            os.kill(mpp["decoder"].pid, signal.SIGTERM)
            mpp["decoder"].join()
            mpp["decoder"] = None
            logger.info(whoami() + "decoder terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 4. clear mp_work_queue
    logger.debug(whoami() + "clearing mp_work_queue")
    while True:
        try:
            mp_work_queue.get_nowait()
        except (queue.Empty, EOFError):
            break
    logger.info(whoami() + "mp_work_queue cleared!")
    try:
        if mpp_is_alive(mpp, "unrarer"):
            mpid = mpp["unrarer"].pid
            logger.debug("terminating unrarer")
            os.kill(mpid, signal.SIGTERM)
            mpp["unrarer"].join()
            mpp["unrarer"] = None
            logger.info(whoami() + "unrarer terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 7. stop rar_verifier
    try:
        if mpp_is_alive(mpp, "verifier"):
            mpid = mpp["verifier"].pid
            logger.debug(whoami() + "terminating par_verifier")
            os.kill(mpp["verifier"].pid, signal.SIGTERM)
            mpp["verifier"].join()
            mpp["verifier"] = None
            logger.info(whoami() + "verifier terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 8. stop renamer only if stopall otherwise just pause
    if stopall:
        try:
            if mpp_is_alive(mpp, "renamer"):
                mpid = mpp["renamer"].pid
                logger.debug(whoami() + "stopall: terminating renamer")
                os.kill(mpp["renamer"].pid, signal.SIGTERM)
                mpp["renamer"].join()
                mpp["renamer"] = None
                logger.info(whoami() + "renamer terminated!")
        except Exception as e:
            logger.debug(whoami() + str(e))
    # just pause
    elif pipes:
        try:
            logger.debug(whoami() + "pausing renamer")
            pipes["renamer"][0].send(("pause", None, None))
        except Exception as e:
            logger.warning(whoami() + str(e))
    # 9. stop post-proc
    try:
        if mpp_is_alive(mpp, "post"):
            mpid = mpp["post"].pid
            logger.debug(whoami() + "terminating postprocesspr")
            os.kill(mpid, signal.SIGTERM)
            mpp["post"].join()
            mpp["post"] = None
            logger.info(whoami() + "postprocessor terminated!")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # 10. stop nzbparser
    if stopall:
        try:
            if mpp_is_alive(mpp, "nzbparser"):
                mpid = mpp["nzbparser"].pid
                logger.debug(whoami() + "terminating nzb_parser")
                os.kill(mpp["nzbparser"].pid, signal.SIGTERM)
                mpp["nzbparser"].join()
                mpp["nzbparser"] = None
                logger.info(whoami() + "postprocessor terminated!")
        except Exception as e:
            logger.debug(whoami() + str(e))
    # 11. threads + servers
    if stopall:
        logger.debug(whoami() + "checking termination of connection threads")
        if not CONNECTIONS_AS_MP:
            ct.stop_threads()
        else:
            do_mpconnections(pipes, "stop", None)
    # 12. only stop mpconnector if stopall otherwise just pause
    if stopall and CONNECTIONS_AS_MP:
        try:
            if pipes["mpconnector"]:
                logger.debug(whoami() + "starting shutdown of mpconnector")
                do_mpconnections(pipes, "exit", None)
                mpp["mpconnector"].join()
                mpp["mpconnector"] = None
                logger.info(whoami() + "mpconnector terminated!")
            else:
                if mpp_is_alive(mpp, "mpconnector"):
                    mpid = mpp["mpconnector"].pid
                    logger.debug(whoami() + "stopall: terminating mpconnector")
                    os.kill(mpp["mpconnector"].pid, signal.SIGTERM)
                    mpp["mpconnector"].join()
                    mpp["mpconnector"] = None
                    logger.info(whoami() + "mpconnector terminated!")
                else:
                    logger.info(whoami() + "mpconnector not running / or zombie!")
        except Exception as e:
            logger.debug(whoami() + str(e))
    # just pause
    elif pipes and CONNECTIONS_AS_MP:
        try:
            do_mpconnections(pipes, "pause", None)
            logger.debug(whoami() + "pausing mpconnector / threads")
        except Exception as e:
            logger.warning(whoami() + str(e))

    logger.info(whoami() + "clearing finished")
    return


def connection_thread_health(threads):
    nothreads = len([t for t, _ in threads])
    nodownthreads = len([t for t, _ in threads if t.connectionstate == -1])
    if nothreads == 0:
        return 0
    return 1 - nodownthreads / (nothreads)


# updates file modification time of nzbfile in order to have them re-read by nzbparser
def update_fmodtime_nzbfiles(nzbfilelist, dirs, logger):
    for nzbfile in nzbfilelist:
        nzbfile_full = dirs["nzb"] + nzbfile
        try:
            f = open(nzbfile_full, "a")   # trigger inotify "MODIFY"
            f.write("<!--modified-->\n")
            f.close()
        except Exception as e:
            logger.warning(whoami() + str(e))
    return


def remove_nzbdirs(deleted_nzbs, dirs, pwdb, logger, removenzbfile=True):
    for deleted_nzb in deleted_nzbs:
        nzbdirname = re.sub(r"[.]nzb$", "", deleted_nzb, flags=re.IGNORECASE) + "/"
        if removenzbfile:
            # delete nzb from .ginzibix/nzb
            try:
                os.remove(dirs["nzb"] + deleted_nzb)
                logger.debug(whoami() + ": deleted NZB " + deleted_nzb + " from NZB dir")
            except Exception as e:
                logger.warning(whoami() + str(e))
        # remove incomplete/$nzb_name
        try:
            shutil.rmtree(dirs["incomplete"] + nzbdirname)
            logger.debug(whoami() + ": deleted incomplete dir for " + deleted_nzb)
        except Exception as e:
            logger.warning(whoami() + str(e))


# main loop for ginzibix downloader
def ginzi_main(cfg_file, cfg, dirs, subdirs, guiport, mp_loggerqueue):

    global CONNECTIONS_AS_MP

    setproctitle("gzbx." + os.path.basename(__file__))

    logger = setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")

    pwdb = PWDBSender()

    # connections in main thread or as mp?
    try:
        CONNECTIONS_AS_MP = True if cfg["OPTIONS"]["CONNECTIONS_AS_MP"].lower() == "yes" else False
    except Exception as e:
        logger.warning(whoami() + str(e) + ", setting connections_as_mp to False")

    # multiprocessing events
    mp_events = {}
    mp_events["unrarer"] = mp.Event()
    mp_events["verifier"] = mp.Event()

    # threading events
    event_stopped = threading.Event()

    if not CONNECTIONS_AS_MP:
        articlequeue = deque()
        resultqueue = deque()
    else:
        articlequeue = None
        resultqueue = None
    mp_work_queue = mp.Queue()
    renamer_result_queue = mp.Queue()

    # filewrite_lock = mp.Lock()
    mpconnector_lock = threading.Lock()
    filewrite_lock = mp.Lock()

    renamer_parent_pipe, renamer_child_pipe = mp.Pipe()
    unrarer_parent_pipe, unrarer_child_pipe = mp.Pipe()
    verifier_parent_pipe, verifier_child_pipe = mp.Pipe()
    mpconnector_parent_pipe, mpconnector_child_pipe = mp.Pipe()
    pipes = {"renamer": [renamer_parent_pipe, renamer_child_pipe],
             "unrarer": [unrarer_parent_pipe, unrarer_child_pipe],
             "verifier": [verifier_parent_pipe, verifier_child_pipe],
             "mpconnector": [mpconnector_parent_pipe, mpconnector_child_pipe, mpconnector_lock]}

    # load server ts from file
    try:
        server_ts0 = pickle.load(open(dirs["main"] + "ginzibix.ts", "rb"))
    except Exception:
        server_ts0 = {}
    config_servers = get_configured_servers(cfg)
    config_servers.append("-ALL SERVERS-")
    server_ts = {key: server_ts0[key] for key in server_ts0 if key in config_servers}
    del server_ts0

    if not CONNECTIONS_AS_MP:
        ct = ConnectionThreads(cfg, articlequeue, resultqueue, server_ts, logger)
    else:
        ct = None

    # update delay
    try:
        update_delay = float(cfg["OPTIONS"]["UPDATE_DELAY"])
    except Exception as e:
        logger.warning(whoami() + str(e) + ", setting update_delay to default 0.5")
        update_delay = 0.5

    # init tcp with gtkgui.py
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:" + str(guiport))
    socket.setsockopt(zmq.RCVTIMEO, int(update_delay * 1000))

    # init sighandler
    logger.debug(whoami() + "initializing sighandler")
    mpp = {"nzbparser": None, "decoder": None, "unrarer": None, "renamer": None, "verifier": None, "post": None,
           "mpconnector": None}
    sh = SigHandler_Main(event_stopped, logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    # start nzb parser mpp
    logger.info(whoami() + "starting nzbparser process ...")
    mpp_nzbparser = mp.Process(target=ParseNZB, args=(cfg, dirs, mp_loggerqueue, ))
    mpp_nzbparser.start()
    mpp["nzbparser"] = mpp_nzbparser

    # start renamer
    logger.info(whoami() + "starting renamer process ...")
    mpp_renamer = mp.Process(target=renamer, args=(renamer_child_pipe, renamer_result_queue, mp_loggerqueue, filewrite_lock, ))
    mpp_renamer.start()
    mpp["renamer"] = mpp_renamer

    if CONNECTIONS_AS_MP:
        # start mpconnector
        logger.info(whoami() + "starting mpconnector process ...")
        mpp_connector = mp.Process(target=mpconnector, args=(mpconnector_child_pipe, cfg, server_ts, mp_loggerqueue, ))
        mpp_connector.start()
        mpp["mpconnector"] = mpp_connector

    dl = None
    nzbname = None
    paused = False
    article_health = 0
    connection_health = 0

    dl_running = True
    applied_datarec = None

    # reload nzb lists for gui
    pwdb.exc("store_sorted_nzbs", [], {})

    DEBUGPRINT = False

    # main looooooooooooooooooooooooooooooooooooooooooooooooooooop
    try:
        while not event_stopped.is_set():
            # set connection health
            if dl:
                stat0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
                if stat0 == 2:
                    statusmsg = "downloading"
                elif stat0 == 3:
                    statusmsg = "postprocessing"
                elif stat0 == 4:
                    statusmsg = "success"
                elif stat0 == -4:
                    statusmsg = "failed"
                # send data to gui
                if not CONNECTIONS_AS_MP:
                    connection_health = connection_thread_health(ct.threads)
                else:
                    connection_health = do_mpconnections(pipes, "connection_thread_health", None)
            else:
                article_health = 0
                connection_health = 0
                statusmsg = ""

            msg = None
            datarec = None
            try:
                msg, datarec = socket.recv_pyobj()
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    msg = None
                    pass
            except Exception as e:
                logger.error(whoami() + str(e))
                try:
                    socket.send_pyobj(("NOOK", None))
                except Exception as e:
                    logger.error(whoami() + str(e))
            if msg:
                if DEBUGPRINT:
                    print("-" * 10, "received", msg)
            if msg == "REQ":
                try:
                    if DEBUGPRINT:
                        print(">>>> #0 main:", time.time(), msg)
                    if CONNECTIONS_AS_MP:
                        serverconfig = do_mpconnections(pipes, "get_server_config", None)
                    else:
                        if not ct.servers:
                            serverconfig = None
                        else:
                            serverconfig = ct.servers.server_config
                    try:
                        update_server_ts(server_ts, ct, pipes)
                    except Exception as e:
                        logger.warning(whoami() + str(e))
                    full_data_for_gui = pwdb.exc("get_all_data_for_gui", [], {})
                    sorted_nzbs, sorted_nzbshistory = pwdb.exc("get_stored_sorted_nzbs", [], {})
                    if dl:
                        dl_results = dl.results
                    else:
                        dl_results = None
                    getdata = None
                    downloaddata_gc = None
                    if dl_results:
                        nzbname, downloaddata, _, _ = dl_results
                        if DEBUGPRINT:
                            print(">>>> #0a main:", time.time(), msg)
                        bytescount0, allbytesdownloaded0, availmem0, avgmiblist, filetypecounter, _, article_health,\
                            overall_size, already_downloaded_size, p2, overall_size_wparvol, allfileslist = downloaddata
                        gb_downloaded = dl.allbytesdownloaded0 / GB_DIVISOR
                        if DEBUGPRINT:
                            print(">>>> #0b main:", time.time(), msg)
                        downloaddata_gc = bytescount0, availmem0, avgmiblist, filetypecounter, nzbname, article_health,\
                            overall_size, already_downloaded_size
                        if DEBUGPRINT:
                            print(">>>> #4 main:", time.time(), msg)
                        getdata = downloaddata_gc, serverconfig, dl_running, statusmsg,\
                            sorted_nzbs, sorted_nzbshistory, article_health, connection_health, dl.serverhealth(),\
                            full_data_for_gui, gb_downloaded, server_ts
                    else:
                        downloaddata_gc = None, None, None, None, None, None, None, None
                        getdata = downloaddata_gc, serverconfig, dl_running, statusmsg, \
                            sorted_nzbs, sorted_nzbshistory, 0, 0, None, full_data_for_gui, 0, server_ts
                        # if one element in getdata != None - send:
                    if getdata.count(None) != len(getdata) or downloaddata_gc.count(None) != len(downloaddata_gc):
                        sendtuple = ("DL_DATA", getdata)
                    else:
                        sendtuple = ("NOOK", None)
                except Exception as e:
                    logger.error(whoami() + str(e))
                    sendtuple = ("NOOK", None)
                try:
                    socket.send_pyobj(sendtuple)
                except Exception as e:
                    logger.error(whoami() + str(e))
                    if DEBUGPRINT:
                        print(str(e))
            elif msg == "NZB_ADDED":
                for nzb0 in datarec:
                    try:
                        shutil.copy(nzb0, dirs["nzb"])
                        socket.send_pyobj(("NZB_ADDED_OK", None))
                    except Exception as e:
                        logger.error(whoami() + str(e))
                logger.info(whoami() + "copied new nzb files into nzb_dir")
            elif msg == "SET_CLOSEALL":
                try:
                    socket.send_pyobj(("SET_CLOSE_OK", None))
                    applied_datarec = datarec
                    event_stopped.set()
                    continue
                except Exception as e:
                    logger.error(whoami() + str(e))
            elif msg == "SET_PAUSE":     # pause downloads
                try:
                    if not paused:
                        paused = True
                        logger.info(whoami() + "download paused for NZB " + nzbname)
                        if not CONNECTIONS_AS_MP:
                            ct.pause_threads()
                        else:
                            do_mpconnections(pipes, "pause", None)
                        if dl:
                            dl.pause()
                        postproc_pause()
                    socket.send_pyobj(("SET_PAUSE_OK", None))
                    dl_running = False
                except Exception as e:
                    logger.error(whoami() + str(e))
            elif msg == "SET_RESUME":    # resume downloads
                try:
                    if paused:
                        logger.info(whoami() + "download resumed for NZB " + nzbname)
                        paused = False
                        if not CONNECTIONS_AS_MP:
                            ct.resume_threads()
                        else:
                            do_mpconnections(pipes, "resume", None)
                        if dl:
                            dl.resume()
                        postproc_resume()
                    socket.send_pyobj(("SET_RESUME_OK", None))
                    dl_running = True
                except Exception as e:
                    logger.error(whoami() + str(e))
                continue
            elif msg == "REPROCESS_FROM_LAST":
                try:
                    for reprocessed_nzb in datarec:
                        reproc_stat0 = pwdb.exc("db_nzb_getstatus", [reprocessed_nzb], {})
                        if reproc_stat0:
                            nzbdirname = re.sub(r"[.]nzb$", "", reprocessed_nzb, flags=re.IGNORECASE) + "/"
                            incompletedir = dirs["incomplete"] + nzbdirname
                            # status -1, -2, 4: restart from 0
                            if reproc_stat0 in [-1, -2, 4]:
                                pwdb.exc("db_nzb_delete", [reprocessed_nzb], {})
                                remove_nzbdirs([reprocessed_nzb], dirs, pwdb, logger, removenzbfile=False)
                                update_fmodtime_nzbfiles([reprocessed_nzb], dirs, logger)    # trigger nzbparser.py
                                logger.debug(whoami() + reprocessed_nzb + ": status " + str(reproc_stat0) + ", restart from 0")
                            # status -4/-3 (postproc. failed/interrupted): re-postprocess
                            elif reproc_stat0 in [-4, -3]:
                                if stat0 == -4:
                                    pwdb.exc("db_nzb_undo_postprocess", [nzbname], {})
                                    clear_postproc_dirs(nzbname, dirs)
                                #  if incompletedir: -> postprocess again
                                if os.path.isdir(incompletedir):
                                    pwdb.exc("nzb_prio_insert_second", [reprocessed_nzb, 3], {})
                                    logger.debug(whoami() + reprocessed_nzb + ": status -4/-3 w/ dir, restart from 3")
                                # else restart overall
                                else:
                                    pwdb.exc("db_nzb_delete", [reprocessed_nzb], {})
                                    remove_nzbdirs([reprocessed_nzb], dirs, pwdb, logger, removenzbfile=False)
                                    update_fmodtime_nzbfiles([reprocessed_nzb], dirs, logger)
                                    logger.debug(whoami() + reprocessed_nzb + ": status -4/-3 w/o dir, restart from 0")
                            # else: undefined
                            else:
                                logger.debug(whoami() + reprocessed_nzb + ": status " + str(reproc_stat0) + ", no action!")
                    pwdb.exc("store_sorted_nzbs", [], {})
                    socket.send_pyobj(("REPROCESS_FROM_START_OK", None))
                except Exception as e:
                    logger.error(whoami() + str(e))
            elif msg in ["DELETED_FROM_HISTORY", "REPROCESS_FROM_START"]:
                try:
                    for removed_nzb in datarec:
                        pwdb.exc("db_nzb_delete", [removed_nzb], {})
                    pwdb.exc("store_sorted_nzbs", [], {})
                    if msg == "DELETED_FROM_HISTORY":
                        remove_nzbdirs(datarec, dirs, pwdb, logger)
                        socket.send_pyobj(("DELETED_FROM_HISTORY_OK", None))
                        logger.info(whoami() + "NZBs have been deleted from history")
                    else:
                        remove_nzbdirs(datarec, dirs, pwdb, logger, removenzbfile=False)
                        update_fmodtime_nzbfiles(datarec, dirs, logger)    # trigger nzbparser.py
                        socket.send_pyobj(("REPROCESS_FROM_START_OK", None))
                        logger.info(whoami() + "NZBs will be reprocessed from start")
                except Exception as e:
                    logger.error(whoami() + str(e))
            elif msg == "SET_NZB_INTERRUPT":
                logger.info(whoami() + "NZBs have been stopped/moved to history")
                try:
                    first_has_changed, moved_nzbs = pwdb.exc("move_nzb_list", [datarec], {"move_and_resetprios": False})
                    if moved_nzbs:
                        pwdb.exc("db_msg_insert", [nzbname, "NZB(s) moved to history", "warning"], {})
                    if first_has_changed:
                        logger.info(whoami() + "first NZB has changed")
                        if dl:
                            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False)
                            dl.stop()
                            dl.join()
                        first_has_changed, moved_nzbs = pwdb.exc("move_nzb_list", [datarec], {"move_and_resetprios": True})
                        nzbname = None
                        if dl:
                            del dl
                            dl = None
                    else:    # if current nzb didnt change just update, but do not restart
                        first_has_changed, moved_nzbs = pwdb.exc("move_nzb_list", [datarec], {"move_and_resetprios": True})
                    pwdb.exc("store_sorted_nzbs", [], {})
                    socket.send_pyobj(("SET_INTERRUPT_OK", None))
                except Exception as e:
                    logger.error(whoami() + str(e))
            elif msg == "SET_NZB_ORDER":
                logger.info(whoami() + "NZBs have been reordered/deleted")
                try:
                    # just get info if first has changed etc.
                    first_has_changed, deleted_nzbs = pwdb.exc("reorder_nzb_list", [datarec], {"delete_and_resetprios": False})
                    if deleted_nzbs:
                        pwdb.exc("db_msg_insert", [nzbname, "NZB(s) deleted", "warning"], {})
                    if first_has_changed:
                        logger.info(whoami() + "first NZB has changed")
                        if dl:
                            clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False)
                            dl.stop()
                            dl.join()
                        first_has_changed, deleted_nzbs = pwdb.exc("reorder_nzb_list", [datarec], {"delete_and_resetprios": True})
                        remove_nzbdirs(deleted_nzbs, dirs, pwdb, logger)
                        nzbname = None
                        if dl:
                            del dl
                            dl = None
                    else:    # if current nzb didnt change just update, but do not restart
                        first_has_changed, deleted_nzbs = pwdb.exc("reorder_nzb_list", [datarec], {"delete_and_resetprios": True})
                        remove_nzbdirs(deleted_nzbs, dirs, pwdb, logger)
                    pwdb.exc("store_sorted_nzbs", [], {})
                    # release gtkgui from block
                    socket.send_pyobj(("SET_DELETE_REORDER_OK", None))
                except Exception as e:
                    logger.error(whoami() + str(e))
            elif msg:
                try:
                    socket.send_pyobj(("NOOK", None))
                except Exception as e:
                    if DEBUGPRINT:
                        print(str(e))
                    logger.debug(whoami() + str(e) + ", received msg: " + str(msg))
                continue

            # if not downloading
            if not dl:
                nzbname = make_allfilelist_wait(pwdb, dirs, logger, -1)
                if nzbname:
                    if not CONNECTIONS_AS_MP:
                        ct.reset_timestamps_bdl()
                    else:
                        do_mpconnections(pipes, "reset_timestamps_bdl", None)
                    logger.info(whoami() + "got next NZB: " + str(nzbname))
                    dl = Downloader(cfg, dirs, ct, mp_work_queue, articlequeue, resultqueue, mpp, pipes,
                                    renamer_result_queue, mp_events, nzbname, mp_loggerqueue, filewrite_lock, logger)
                    # if status postprocessing, don't start threads!
                    if pwdb.exc("db_nzb_getstatus", [nzbname], {}) in [0, 1, 2]:
                        if not paused:
                            if CONNECTIONS_AS_MP:
                                do_mpconnections(pipes, "resume", None)
                            else:
                                ct.resume_threads()
                        if paused:
                            dl.pause()
                        dl.start()
            else:
                stat0 = pwdb.exc("db_nzb_getstatus", [nzbname], {})
                # if postproc ok
                if stat0 == 4:
                    logger.info(whoami() + "postprocessor success for NZB " + nzbname)
                    if not CONNECTIONS_AS_MP:
                        ct.pause_threads()
                    else:
                        do_mpconnections(pipes, "pause", None)
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger, stopall=False)
                    
                    dl.stop()
                    dl.join()
                    if mpp_is_alive(mpp, "post"):
                        mpp["post"].join()
                        mpp["post"] = None
                    pwdb.exc("db_msg_insert", [nzbname, "downloaded and postprocessed successfully!", "success"], {})
                    # set 'flags' for getting next nzb
                    del dl
                    dl = None
                    nzbname = None
                    pwdb.exc("store_sorted_nzbs", [], {})
                # if download ok -> postprocess
                elif stat0 == 3 and not mpp_is_alive(mpp, "post"):
                    article_health = 0
                    connection_health = 0
                    logger.info(whoami() + "download success, postprocessing NZB " + nzbname)
                    mpp_post = mp.Process(target=postprocess_nzb, args=(nzbname, articlequeue, resultqueue, mp_work_queue, pipes, mpp, mp_events, cfg,
                                                                        dl.verifiedrar_dir, dl.unpack_dir, dl.nzbdir, dl.rename_dir, dl.main_dir,
                                                                        dl.download_dir, dl.dirs, dl.pw_file, mp_loggerqueue, ))
                    mpp_post.start()
                    mpp["post"] = mpp_post
                # if download failed
                elif stat0 == -2:
                    logger.info(whoami() + "download failed for NZB " + nzbname)
                    if not CONNECTIONS_AS_MP:
                        ct.pause_threads()
                    else:
                        do_mpconnections(pipes, "pause", None)
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger,
                                   stopall=False, onlyarticlequeue=False)
                    dl.stop()
                    dl.join()
                    # set 'flags' for getting next nzb
                    del dl
                    dl = None
                    nzbname = None
                    pwdb.exc("store_sorted_nzbs", [], {})
                # if postproc failed
                elif stat0 == -4:
                    logger.error(whoami() + "postprocessor failed for NZB " + nzbname)
                    if CONNECTIONS_AS_MP:
                        do_mpconnections(pipes, "pause", None)
                    else:
                        ct.pause_threads()
                    clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger,
                                   stopall=False, onlyarticlequeue=False)
                    dl.stop()
                    dl.join()
                    if mpp_is_alive(mpp, "post"):
                        mpp["post"].join()
                    pwdb.exc("db_msg_insert", [nzbname, "downloaded and/or postprocessing failed!", "error"], {})
                    mpp["post"] = None
                    # set 'flags' for getting next nzb
                    del dl
                    dl = None
                    nzbname = None
                    pwdb.exc("store_sorted_nzbs", [], {})
    except Exception as e:
        if DEBUGPRINT:
            print(str(e))
        else:
            pass
    # shutdown
    logger.info(whoami() + "closeall: starting shutdown sequence")
    if CONNECTIONS_AS_MP:
        do_mpconnections(pipes, "pause", None)
    else:
        ct.pause_threads()
    logger.debug(whoami() + "closeall: connection threads paused")
    if dl:
        dl.stop()
        dl.join()
    logger.debug(whoami() + "closeall: downloader joined")
    try:
        clear_download(nzbname, pwdb, articlequeue, resultqueue, mp_work_queue, dl, dirs, pipes, mpp, ct, logger,
                       stopall=True, onlyarticlequeue=False)
    except Exception as e:
        logger.error(whoami() + str(e) + ": closeall error!")
    dl = None
    logger.debug(whoami() + "closeall: closing gtkgui-socket")
    try:
        socket.close()
        context.term()
    except Exception as e:
        logger.warning(whoami() + str(e))
    logger.debug(whoami() + "closeall: all cleared")
    # save pandas time series
    try:
        pickle.dump(server_ts, open(dirs["main"] + "ginzibix.ts", "wb"))
        logger.info(whoami() + "closeall: saved downloaded-timeseries to file")
    except Exception as e:
        logger.warning(whoami() + str(e) + ": closeall: error in saving download-timeseries")
    # if restart because of settings applied in gui -> write cfg to file
    if applied_datarec:
        try:
            with open(cfg_file, 'w') as configfile:
                applied_datarec.write(configfile)
            logger.debug(whoami() + "changed config file written!")
        except Exception as e:
            logger.error(whoami() + str(e) + ": cannot write changed config file!")
    logger.info(whoami() + "exited!")
    sys.exit(0)
