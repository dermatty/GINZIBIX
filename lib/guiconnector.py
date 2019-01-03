from threading import Thread
from .aux import PWDBSender
from statistics import mean
import zmq
import os
import time
import re
import shutil
import datetime
import sys
import inspect


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    return "guiconnector_thread / " + outer_func_name + " / #" + outer_func_linenr + ": "


def remove_nzb_files_and_db(deleted_nzb_name0, dirs, pwdb, logger):
    nzbdirname = re.sub(r"[.]nzb$", "", deleted_nzb_name0, flags=re.IGNORECASE) + "/"
    # delete nzb from .ginzibix/nzb0
    try:
        os.remove(dirs["nzb"] + deleted_nzb_name0)
        logger.debug(whoami() + ": deleted NZB " + deleted_nzb_name0 + " from NZB dir")
    except Exception as e:
        logger.debug(whoami() + str(e))
    # remove from db
    pwdb.exc("db_nzb_delete", [deleted_nzb_name0], {})
    # pwdb.db_nzb_delete(deleted_nzb_name0)
    # remove incomplete/$nzb_name
    try:
        shutil.rmtree(dirs["incomplete"] + nzbdirname)
        logger.debug(whoami() + ": deleted incomplete dir for " + deleted_nzb_name0)
    except Exception as e:
        logger.debug(whoami() + str(e))


class GUI_Connector(Thread):
    def __init__(self, lock, dirs, logger, cfg):
        Thread.__init__(self)
        self.daemon = True
        self.dirs = dirs
        self.pwdb = PWDBSender()
        self.cfg = cfg
        try:
            self.port = self.cfg["OPTIONS"]["PORT"]
            assert(int(self.port) > 1024 and int(self.port) <= 65535)
        except Exception as e:
            self.logger.debug(whoami() + str(e) + ", setting port to default 36603")
            self.port = "36603"
        self.logger = logger
        self.lock = lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + self.port)
        self.clear_data()

    def clear_data(self):
        with self.lock:
            self.data = None
            self.nzbname = None
            self.threads = []
            self.server_config = None
            self.dl_running = True
            self.status = "idle"
            self.order_has_changed = False
            self.first_has_changed = False
            self.deleted_nzb_name = None
            self.old_t = 0
            self.oldbytes0 = 0
            self.sorted_nzbs = None
            self.sorted_nzbshistory = None
            self.first_idle_pass = True
            self.dlconfig = None
            self.netstatlist = []
            self.last_update_for_gui = datetime.datetime.now()
            self.closeall = False
            self.article_health = 0
            self.connection_health = 0
            self.connectionthreads = []
            self.oldret0 = (None, None, None, None, None, None, None, None, None, None, None, None)
            try:
                self.update_delay = float(self.cfg["GTKGUI"]["UPDATE_DELAY"])
            except Exception as e:
                self.logger.debug(whoami() + str(e) + ": setting update delay to 0.5 sec")
                self.update_delay = 0.5

    def set_health(self, article_health, connection_health):
        with self.lock:
            self.article_health = article_health
            self.connection_health = connection_health

    def set_data(self, data, threads, server_config, status, dlconfig):
        with self.lock:
            if data:
                bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
                self.data = data
                self.nzbname = nzbname
                self.server_config = server_config
                self.status = status
                self.dlconfig = dlconfig
                self.threads = []
                self.connectionthreads = threads

    def get_netstat(self):
        bytes0 = 0
        self.threads = []
        for t, last_timestamp in self.connectionthreads:
            append_tuple = (t.bytesdownloaded, t.last_timestamp, t.idn, t.bandwidth_bytes)
            self.threads.append(append_tuple)
            bytes0 += t.bytesdownloaded
        if bytes0 > 0:
            dt = time.time() - self.old_t
            if dt == 0:
                dt = 0.001
            mbitcurr = ((bytes0 - self.oldbytes0) / dt) / (1024 * 1024) * 8
            self.oldbytes0 = bytes0
            self.old_t = time.time()
            self.netstatlist = [(mbit, t) for mbit, t in self.netstatlist if time.time() - t <= 2.0] + [(mbitcurr, self.old_t)]
            return mean([mbit for mbit, _ in self.netstatlist])

        else:
            return 0

    def get_data(self):
        ret0 = (None, None, None, None, None, None, None, None, None, None, None, None)
        # return also values if a message has been inserted
        if not self.pwdb.exc("db_msg_get_last_update", [], {}) > self.last_update_for_gui:
            if self.pwdb.exc("db_nzb_are_all_nzb_idle", [], {}):
                if not self.first_idle_pass:
                    return ret0
                else:
                    self.first_idle_pass = False
            else:
                self.first_idle_pass = True
        with self.lock:
            self.full_data_for_gui = self.pwdb.exc("get_all_data_for_gui", [], {})
            self.sorted_nzbs, self.sorted_nzbshistory = self.pwdb.exc("get_stored_sorted_nzbs", [], {})
            self.mean_netstat = self.get_netstat()
            try:
                ret1 = (self.data, self.server_config, self.threads, self.dl_running, self.status,
                        self.mean_netstat, self.sorted_nzbs, self.sorted_nzbshistory, self.article_health, self.connection_health,
                        self.dlconfig, self.full_data_for_gui)
                match_ret = [i for i, (x, y) in enumerate(zip(self.oldret0, ret1)) if x != y]
                if match_ret:
                    ret0 = ret1
                    self.oldret0 = ret0
                    self.last_update_for_gui = datetime.datetime.now()
            except Exception as e:
                self.logger.warning(whoami() + str(e))
        return ret0

    def has_first_nzb_changed(self):
        res = self.first_has_changed
        self.first_has_changed = False
        return res

    def has_nzb_been_deleted(self, delete=False):
        res = self.deleted_nzb_name
        if delete:
            self.deleted_nzb_name = None
        return res

    def has_order_changed(self):
        res = self.order_has_changed
        self.order_has_changed = False
        return res

    def all_closed(self):
        res = self.closeall
        with self.lock:
            self.closeall = False
        return res

    def run(self):
        stopped = False
        while not stopped:
            try:
                msg, datarec = self.socket.recv_pyobj()
            except Exception as e:
                self.logger.error(whoami() + str(e))
                try:
                    self.socket.send_pyobj(("NOOK", None))
                except Exception as e:
                    self.logger.error(whoami() + str(e))
            if msg == "REQ":
                try:
                    getdata = self.get_data()
                    # if one element in getdata has changed - send:
                    if getdata.count(None) != len(getdata):
                        sendtuple = ("DL_DATA", getdata)
                    else:
                        sendtuple = ("NOOK", None)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                try:
                    self.socket.send_pyobj(sendtuple)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
            elif msg == "SET_CLOSEALL":
                try:
                    self.socket.send_pyobj(("SET_CLOSE_OK", None))
                    with self.lock:
                        self.closeall = True
                    stopped = True
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            elif msg == "SET_PAUSE":     # pause downloads
                try:
                    self.socket.send_pyobj(("SET_PAUSE_OK", None))
                    with self.lock:
                        self.dl_running = False
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            elif msg == "SET_RESUME":    # resume downloads
                try:
                    self.socket.send_pyobj(("SET_RESUME_OK", None))
                    self.dl_running = True
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            elif msg == "SET_DELETE":
                self.order_has_changed = True
                try:
                    self.socket.send_pyobj(("SET_DELETE_OK", None))
                    # with self.lock:
                    first_has_changed0, deleted_nzb_name0 = self.pwdb.exc("set_nzbs_prios", [datarec], {"delete": True})
                    if deleted_nzb_name0 and not first_has_changed0:
                        with self.lock:
                            remove_nzb_files_and_db(deleted_nzb_name0, self.dirs, self.pwdb, self.logger)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                if first_has_changed0:
                    self.first_has_changed = first_has_changed0
                    self.deleted_nzb_name = deleted_nzb_name0
                continue
            elif msg == "SET_NZB_ORDER":
                self.order_has_changed = True
                try:
                    self.socket.send_pyobj(("SET_NZBORDER_OK", None))
                    with self.lock:
                        self.first_has_changed, _ = self.pwdb.exc("set_nzbs_prios", [datarec], {"delete": False})
                    # self.first_has_changed, _ = self.pwdb.set_nzbs_prios(datarec, delete=False)
                except Exception as e:
                    self.logger.error(whoami() + str(e))
                continue
            else:
                try:
                    self.socket.send_pyobj(("NOOK", None))
                except Exception as e:
                    self.logger.debug(whoami() + str(e) + ", received msg: " + str(msg))
                continue

        # close socket & exit guiconnector
        self.logger.debug(whoami() + "closing socket")
        try:
            self.socket.close()
            self.context.term()
        except Exception as e:
            self.logger.warning(whoami())
        # wait for closeall received by main
        self.logger.debug(whoami() + "waiting for main to accept closeall")
        while self.closeall:
            time.sleep(0.1)
        self.logger.info(whoami() + "exiting")
        sys.exit()
