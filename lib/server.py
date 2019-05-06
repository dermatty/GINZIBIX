import ssl
import nntplib
from .mplogging import whoami
from .aux import get_server_config


# Does all the server stuff (open, close connctions) and contains all relevant
# data
class Servers():

    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        # server_config = [(server_name, server_url, user, password, port, usessl, level, connections,
        #                   retention, useserver)]
        self.server_config = get_server_config(self.cfg)
        # all_connections = [(server_name, conn#, retention, nntp_obj)]
        self.all_connections = self.get_all_connections()
        # level_servers0 = {"0": ["EWEKA", "BULK"], "1": ["TWEAK"], "2": ["NEWS", "BALD"]}
        self.level_servers = self.get_level_servers()

    def __bool__(self):
        if not self.server_config:
            return False
        return True

    def get_single_server_config(self, server_name0):
        for server_name, server_url, user, password, port, usessl, level, connections, retention, useserver in self.server_config:
            if server_name == server_name0:
                return server_name, server_url, user, password, port, usessl, level, connections, retention, useserver
        return None

    def get_all_connections(self):
        conn = []
        for s_name, _, _, _, _, _, _, s_connections, s_retention, s_useserver in self.server_config:
            for c in range(s_connections):
                if s_useserver:
                    conn.append((s_name, c + 1, s_retention, None))
        return conn

    def get_level_servers(self):
        s_tuples = []
        for s_name, _, _, _, _, _, s_level, _, _, s_useserver in self.server_config:
            if s_useserver:
                s_tuples.append((s_name, s_level))
        sorted_s_tuples = sorted(s_tuples, key=lambda server: server[1])
        ls = {}
        for s_name, s_level in sorted_s_tuples:
            if str(s_level) not in ls:
                ls[str(s_level)] = []
            ls[str(s_level)].append(s_name)
        return ls

    def open_connection(self, server_name0, conn_nr):
        result = None
        for idx, (sn, cn, rt, nobj) in enumerate(self.all_connections):
            if sn == server_name0 and cn == conn_nr:
                if nobj:
                    return nobj
                else:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                    sc = self.get_single_server_config(server_name0)
                    if sc:
                        server_name, server_url, user, password, port, usessl, level, connections, retention, useserver\
                            = sc
                        if useserver:
                            try:
                                self.logger.debug(whoami() + "Opening connection # " + str(conn_nr) + "to server " + server_name)
                                if usessl:
                                    nntpobj = nntplib.NNTP_SSL(server_url, user=user, password=password, ssl_context=context, port=port, readermode=True)
                                else:
                                    # this is just for ginzicut preloading
                                    if user.lower() == "ginzicut" or password.lower() == "ginzicut":
                                        nntpobj = nntplib.NNTP(server_url, port=port)
                                    else:
                                        nntpobj = nntplib.NNTP(server_url, user=user, password=password, readermode=True, port=port)
                                self.logger.debug(whoami() + "Opened Connection #" + str(conn_nr) + " on server " + server_name0)
                                result = nntpobj
                                self.all_connections[idx] = (sn, cn, rt, nntpobj)
                                break
                            except Exception as e:
                                self.logger.error(whoami() + "Server " + server_name0 + " connect error: " + str(e))
                                self.all_connections[idx] = (sn, cn, rt, None)
                                break
                    else:
                        self.logger.error(whoami() + "Cannot get server config for server: " + server_name0)
                        self.all_connections[idx] = (sn, cn, rt, None)
                        break
        return result

    def close_connection(self, server_name0, conn_nr):
        res = False
        for idx, (sn, cn, rt, nobj) in enumerate(self.all_connections):
            if sn == server_name0 and cn == conn_nr:
                if nobj:
                    try:
                        self.logger.warning(whoami() + "Closed connection #" + str(cn) + " on " + sn)
                        self.all_connections[idx] = (sn, cn, rt, None)
                        nobj.quit()
                        res = True
                    except Exception as e:
                        self.logger.error(whoami() + "Server " + server_name0 + " close error: " + str(e))
                        res = False
                    break
        return res

    def close_all_connections(self):
        for idx, (sn, cn, rt, nobj) in enumerate(self.all_connections):
            if nobj:
                try:
                    nobj.quit()
                    self.all_connections[idx] = (sn, cn, rt, None)
                    self.logger.warning(whoami() + "Closed connection #" + str(cn) + " on " + sn)
                except Exception as e:
                    self.logger.warning(whoami() + "Cannot quit server " + sn + ": " + str(e))

