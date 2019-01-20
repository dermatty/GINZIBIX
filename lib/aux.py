import zmq
import time
from os.path import expanduser


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
                # self.socket.setsockopt(zmq.RCVTIMEO, 500)
                ipc_location = self.maindir + "ginzibix_socket1"
                socketurl = "ipc://" + ipc_location
                # socketurl = "tcp://127.0.0.1:37705"
                self.socket.connect(socketurl)
            except Exception as e:
                self.socket = None
                self.context = None
                return None
        return True

    def reconnect(self, funcstr):
        i = 1
        while True:
            print(funcstr, "Try #", i)
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
                # if a == 0:
                    # print(funcstr, "finally sent")
                break
            except zmq.ZMQError:
                # print(funcstr, "zmq error, trying to reconnect")
                res = self.reconnect(funcstr)
                # print(funcstr, "reconnect success")
                a = 0
            except Exception as e:
                self.context = None
                # print(funcstr, "---", str(e), type(e), sys.exc_info())
                return False

        # receive
        try:
            ret0 = self.socket.recv_pyobj()
            # if (a == 0):
            #     print(funcstr, "finally received", ret0)
            if ret0 == "NOOK":
                return False
            return ret0
        except Exception as e:
            # print(funcstr, "******", str(e), type(e))
            self.context = None
            return False
