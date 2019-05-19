import zmq
import socket
import time
import queue
import threading
import sys
import gi
import re
import os
import shutil
from gi.repository import GLib
from os.path import expanduser
from threading import Thread
from .mplogging import whoami
import signal

gi.require_version('Gtk', '3.0')


def do_mpconnections(pipes, cmd, param):
    res = None
    pipes["mpconnector"][2].acquire()
    try:
        pipes["mpconnector"][0].send((cmd, param))
        if pipes["mpconnector"][0].poll(timeout=3):
            res = pipes["mpconnector"][0].recv()
        else:
            res = None
    except Exception:
        res = None
    pipes["mpconnector"][2].release()
    return res


def clear_postproc_dirs(nzbname, dirs):
    # clear verified_rardir, unpackdir
    nzbdirname = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
    cleardirs = [dirs["incomplete"] + nzbdirname + "_verifiedrars0", dirs["incomplete"] + nzbdirname + "_unpack0"]
    for d in cleardirs:
        for filename in os.listdir(d):
            filepath = os.path.join(d, filename)
            try:
                shutil.rmtree(filepath)
            except OSError:
                os.remove(filepath)
    return


def get_cut_nzbname(nzbname, max_nzb_len):
    if len(nzbname) <= max_nzb_len:
        return nzbname
    else:
        return nzbname[:max_nzb_len - 8] + "[..]" + nzbname[-4:]


def get_cut_msg(msg, max_nzb_len):
    if len(msg) <= max_nzb_len:
        return msg
    else:
        return msg[:max_nzb_len - 8] + "[..]" + msg[-4:]


def get_bg_color(n_status_s):
    bgcol = "white"
    if n_status_s == "preprocessing":
        bgcol = "beige"
    elif n_status_s == "queued":
        bgcol = "khaki"
    elif n_status_s == "downloading":
        bgcol = "yellow"
    elif n_status_s == "postprocessing":
        bgcol = "yellow green"
    elif n_status_s == "success":
        bgcol = "lime green"
    elif n_status_s == "failed" or n_status_s == "unknown":
        bgcol = "red"
    else:
        bgcol = "white"
    return bgcol


def get_status_name_and_color(status):
    if status == 0:
        return "preprocessing", "white"
    if status == 1:
        return "queued", "beige"
    if status == -1:
        return "preprocessing failed", "red"
    if status == 2:
        return "downloading", "yellow"
    if status == -2:
        return "download failed", "red"
    if status == 3:
        return "postprocessing", "yellow green"
    if status == -3:        # this does not exist!?
        return "postprocessing failed", "red"
    if status == 4:
        return "success", "lime green"
    if status == -4:
        return "postprocessing failed", "red"
    return "N/A", "white"


def mpp_is_alive(mpp, procname):
    try:
        mpp0 = mpp[procname]
    except Exception:
        return False
    try:
        if mpp0.is_alive() and mpp0.pid and not mpp0.exitcode:
            # print(procname, "isalive #0")
            return True
        else:
            return False
    except Exception:
        pass
    # if mpp is not a child, use proc/stat
    try:
        with open("/proc/" + str(mpp0.pid) + "/stat") as p:
            ptype = p.readline().split(" ")[2]
        if ptype == "Z":
            return False
        # print(procname, "isalive #1", ptype)
        return True
    except Exception:
        return False


def kill_mpp(mpp, mppname, timeout=None):
    try:
        if mpp_is_alive(mpp, mppname):
            mpid = mpp[mppname].pid
            os.kill(mpid, signal.SIGTERM)
            mpp[mppname].join(timeout=timeout)
            if mpp_is_alive(mpp, mppname):
                os.kill(mpid, signal.SIGKILL)
                mpp[mppname].join()
            mpp[mppname] = None
        elif mpp[mppname]:
            try:
                mpid = mpp[mppname].pid
                os.kill(mpid, signal.SIGKILL)
            except Exception:
                pass
            mpp[mppname] = None
    except Exception:
        try:
            mpp[mppname] = None
        except Exception:
            pass
    return


def mpp_join(mpp, procname, timeout=-1):
    if timeout == -1:
        timeout0 = 999999999
    else:
        timeout0 = timeout
    t0 = time.time()
    while mpp_is_alive(mpp, procname) and time.time() - t0 < timeout0:
        time.sleep(0.1)


def make_dirs():
    userhome = expanduser("~")
    maindir = userhome + "/.ginzibix/"
    dirs = {
        "userhome": userhome,
        "main": maindir,
        "config": maindir + "config/",
        "nzb": maindir + "nzb/",
        "complete": maindir + "complete/",
        "incomplete": maindir + "incomplete/",
        "logs": maindir + "logs/"
    }
    subdirs = {
        "download": "_downloaded0",
        "renamed": "_renamed0",
        "unpacked": "_unpack0",
        "verififiedrar": "_verifiedrars0"
    }
    return userhome, maindir, dirs, subdirs


def get_free_server_cfg(cfg):
    snr = 0
    idx = 0
    snrstr = ""
    while idx < 99:
        idx += 1
        try:
            snr += 1
            snrstr = "SERVER" + str(snr)
            assert cfg[snrstr]["SERVER_NAME"]
        except Exception:
            break
    return snrstr


# gets only names of configured servers
def get_configured_servers(cfg):
    # get servers from config, max SERVER10
    snr = 0
    idx = 0
    sconf = []
    while idx < 99:
        idx += 1
        try:
            snr += 1
            snrstr = "SERVER" + str(snr)
            server_name = cfg[snrstr]["SERVER_NAME"]
            sconf.append(server_name)
        except Exception:
            continue
    if not sconf:
        return None
    return sconf


def get_config_for_server(servername0, cfg):
    snr = 0
    idx = 0
    result = {}
    snrstr = ""
    while idx < 99:
        idx += 1
        try:
            snr += 1
            snrstr = "SERVER" + str(snr)
            if servername0 == cfg[snrstr]["SERVER_NAME"]:
                result["server_name"] = cfg[snrstr]["server_name"]
                try:
                    result["active"] = cfg[snrstr]["use_server"]
                except Exception:
                    result["active"] = "no"
                try:
                    result["url"] = cfg[snrstr]["server_url"]
                except Exception:
                    result["url"] = ""
                try:
                    result["user"] = cfg[snrstr]["user"]
                except Exception:
                    result["user"] = ""
                try:
                    result["password"] = cfg[snrstr]["password"]
                except Exception:
                    result["password"] = ""
                try:
                    result["ssl"] = cfg[snrstr]["ssl"]
                except Exception:
                    result["ssl"] = "no"
                try:
                    result["port"] = cfg[snrstr]["port"]
                except Exception:
                    result["port"] = "119"
                try:
                    result["level"] = cfg[snrstr]["level"]
                except Exception:
                    result["level"] = "0"
                try:
                    result["connections"] = cfg[snrstr]["connections"]
                except Exception:
                    result["connections"] = "1"
                try:
                    result["retention"] = cfg[snrstr]["retention"]
                except Exception:
                    result["retention"] = "-1"
                break
        except Exception:
            pass
    return snrstr, result


# reads servers from config
def get_server_config(cfg):
    # get servers from config, max SERVER10
    snr = 0
    idx = 0
    sconf = []
    while idx < 99:
        idx += 1
        try:
            snr += 1
            snrstr = "SERVER" + str(snr)
            useserver = True if cfg[snrstr]["USE_SERVER"].lower() == "yes" else False
            server_name = cfg[snrstr]["SERVER_NAME"]
            server_url = cfg[snrstr]["SERVER_URL"]
            user = cfg[snrstr]["USER"]
            password = cfg[snrstr]["PASSWORD"]
            port = int(cfg[snrstr]["PORT"])
            usessl = True if cfg[snrstr]["SSL"].lower() == "yes" else False
            level = int(cfg[snrstr]["LEVEL"])
            connections = int(cfg[snrstr]["CONNECTIONS"])
        except Exception:
            continue
        try:
            retention = int(cfg[snrstr]["RETENTION"])
            if retention == -1:
                retention = 999999
            sconf.append((server_name, server_url, user, password, port, usessl, level, connections, retention, useserver))
        except Exception:
            sconf.append((server_name, server_url, user, password, port, usessl, level, connections, 999999, useserver))
    if not sconf:
        return None
    return sconf


class PWDBSender():
    def __init__(self):
        self.context = None
        self.socket = None
        _, self.maindir, _, _ = make_dirs()

    def connect(self):
        if not self.context:
            try:
                self.context = zmq.Context()
                self.socket = self.context.socket(zmq.REQ)
                self.socket.setsockopt(zmq.LINGER, 0)
                ipc_location = self.maindir + "ginzibix_socket1"
                socketurl = "ipc://" + ipc_location
                self.socket.connect(socketurl)
            except Exception:
                self.socket = None
                self.context = None
                return None
        return True

    def reconnect(self, funcstr):
        i = 1
        while True:
            if self.context:
                self.socket.close()
                self.context = None
            time.sleep(1)
            res = self.connect()
            if res:
                return True
            i += 1

    def exc(self, funcstr, args0, kwargs0):
        res = self.connect()
        if not res:
            return None

        # send
        a = -1
        while True:
            try:
                self.socket.send_pyobj((funcstr, args0, kwargs0))
                break
            except zmq.ZMQError:
                res = self.reconnect(funcstr)
            except Exception:
                self.context = None
                return False

        # receive
        try:
            ret0 = self.socket.recv_pyobj()
            if ret0 == "NOOK":
                return False
            return ret0
        except Exception:
            self.context = None
            return False


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


# connects to guiconnector
class GUI_Poller(Thread):

    def __init__(self, gui, port, delay=0.5):
        Thread.__init__(self)
        self.daemon = True
        self.gui = gui
        self.context = zmq.Context()
        self.host = "127.0.0.1"
        self.port = str(port)
        self.lock = self.gui.lock
        self.data = None
        self.delay = float(delay)
        self.appdata = self.gui.appdata
        self.update_mainwindow = self.gui.update_mainwindow
        self.socket = self.context.socket(zmq.REQ)
        self.logger = self.gui.logger
        self.event_stopped = threading.Event()
        self.guiqueue = self.gui.guiqueue
        self.toggle_buttons = self.gui.toggle_buttons
        self.toggle_buttons_history = self.gui.toggle_buttons_history

    def stop(self):
        self.logger.debug(whoami() + "setting event_stopped")
        self.event_stopped.set()

    def run(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        socketurl = "tcp://" + self.host + ":" + self.port
        self.socket.connect(socketurl)
        dl_running = True
        while not self.event_stopped.wait(self.delay):
            # some button pressed, of which main.py should be informed?
            GLib.idle_add(self.gui.update_logs_and_lists)

            try:
                queue_elem = self.guiqueue.get_nowait()
                self.guiqueue.task_done()
            except (queue.Empty, EOFError, ValueError):
                queue_elem = None
            except Exception as e:
                self.logger.error(whoami() + str(e))
                queue_elem = None

            if queue_elem:
                elem_type, elem_val = queue_elem
                if elem_type == "order_changed":
                    msg0 = "SET_NZB_ORDER"
                    msg0_val = [nzb[0] for nzb in self.appdata.nzbs]
                elif elem_type == "interrupted":
                    msg0 = "SET_NZB_INTERRUPT"
                    msg0_val = [nzb[0] for nzb in self.appdata.nzbs]
                elif elem_type == "closeall":
                    msg0 = "SET_CLOSEALL"
                    msg0_val = elem_val          # non-empty if apply / restart!
                    self.appdata.closeall = True
                elif elem_type == "nzb_added":
                    msg0 = "NZB_ADDED"
                    msg0_val, add_button = elem_val
                elif elem_type == "deleted_from_history":
                    msg0 = "DELETED_FROM_HISTORY"
                    msg0_val = elem_val
                elif elem_type == "reprocess_from_start":
                    msg0 = "REPROCESS_FROM_START"
                    msg0_val = elem_val
                elif elem_type == "reprocess_from_last":
                    msg0 = "REPROCESS_FROM_LAST"
                    msg0_val = elem_val

                elif elem_type == "dl_running":
                    msg0_val = None
                    dl_running_new = elem_val
                    if dl_running != dl_running_new:
                        dl_running = dl_running_new
                        if dl_running:
                            msg0 = "SET_RESUME"
                        else:
                            msg0 = "SET_PAUSE"
                    else:
                        msg0 = None
                else:
                    msg0 = None
                if msg0:
                    try:
                        self.socket.send_pyobj((msg0, msg0_val))
                        datatype, datarec = self.socket.recv_pyobj()
                    except Exception as e:
                        self.logger.error(whoami() + str(e))
                    if elem_type == "nzb_added":
                        add_button.set_sensitive(True)
                    elif elem_type == "closeall":
                        with self.lock:
                            self.appdata.closeall = False
                            self.logger.debug(whoami() + "received main closeall confirm, shutting down guipoller")
                    elif elem_type in ["order_changed", "interrupted"]:
                        GLib.idle_add(self.toggle_buttons)
                        self.logger.debug(whoami() + "order changed/interrupted ok!")
                    elif elem_type in ["deleted_from_history", "reprocess_from_start", "reprocess_from_last"]:
                        GLib.idle_add(self.toggle_buttons_history)
                        self.logger.debug(whoami() + "deleted_from_history/reprocess ok!")
                else:
                    self.logger.error(whoami() + "cannot interpret element in guiqueue")
            else:
                try:
                    self.socket.send_pyobj(("REQ", None))
                    datatype, datarec = self.socket.recv_pyobj()
                    if datatype == "NOOK":
                        continue
                    elif datatype == "DL_DATA":
                        data, server_config, dl_running, nzb_status_string, \
                            article_health, connection_health, dlconfig, gb_downloaded, server_ts = datarec
                        try:
                            GLib.idle_add(self.update_mainwindow, data, server_config, dl_running, nzb_status_string,
                                          article_health, connection_health, dlconfig, gb_downloaded, server_ts)
                            continue
                        except Exception as e:
                            self.logger.debug(whoami() + str(e))
                except Exception as e:
                    self.logger.error(whoami() + str(e))

        # close socket, join queue & exit guipoller
        self.logger.debug(whoami() + "closing socket")
        try:
            self.socket.close()
            self.context.term()
        except Exception:
            self.logger.warning(whoami())
        self.logger.debug(whoami() + "joining gui_queue")
        while True:
            try:
                queue_elem = self.guiqueue.get_nowait()
                self.guiqueue.task_done()
            except (queue.Empty, EOFError, ValueError):
                break
            except Exception as e:
                self.logger.error(whoami() + str(e))
                break
        self.guiqueue.join()
        self.logger.info(whoami() + "exiting")
        sys.exit()
