import zmq
from os.path import expanduser


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

    def exc(self, funcstr, args0, kwargs0):
        if not self.context:
            try:
                self.context = zmq.Context()
                self.socket = self.context.socket(zmq.REQ)
                self.socket.setsockopt(zmq.LINGER, 0)
                # self.socket.setsockopt(zmq.RCVTIMEO, 500)
                ipc_location = self.maindir + "ginzibix_socket1"
                socketurl = "ipc://" + ipc_location
                self.socket.connect(socketurl)
            except Exception as e:
                self.socket = None
                self.context = None
                return None
        # send
        try:
            self.socket.send_pyobj((funcstr, args0, kwargs0))
        except Exception as e:
            self.context = None
            return False
        # receive
        try:
            ret0 = self.socket.recv_pyobj()
            if ret0 == "NOOK":
                return False
            return ret0
        except Exception as e:
            self.context = None
            return False
