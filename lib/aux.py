import zmq
import time


class Logsender():
    def __init__(self, cfg):
        self.cfg = cfg
        self.set_host_port_from_cfg()
        self.context = None
        self.socket = None

    def set_host_port_from_cfg(self):
        try:
            self.port = self.cfg["OPTIONS"]["PORT"]
            assert(int(self.port) > 1024 and int(self.port) <= 65535)
        except Exception:
            self.port = "36603"
        try:
            self.host = self.cfg["OPTIONS"]["HOST"]
        except Exception as e:
            self.host = "127.0.0.1"

    def sendlog(self, nzbname, logmsg, loglevel):
        if not self.context:
            try:
                self.context = zmq.Context()
                self.socket = self.context.socket(zmq.REQ)
                self.socket.setsockopt(zmq.LINGER, 0)
                socketurl = "tcp://" + self.host + ":" + self.port
                self.socket.connect(socketurl)
            except Exception as e:
                self.socket = None
                return None
        msg = (nzbname, logmsg, loglevel, time.time())
        try:
            self.socket.send_pyobj(("LOG", msg))
            datatype, datarec = self.socket.recv_pyobj()
            if datatype == "NOOK":
                return None
            return True
        except Exception as e:
            self.context = None
            return None
