from threading import Thread
import threading
import nntplib
import time
import inspect
from .server import Servers
import socket
import queue

def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


lpref = __name__.split("lib.")[-1] + " - "


# This is the thread worker per connection to NNTP server
class ConnectionWorker(Thread):
    def __init__(self, lock, connection, articlequeue, resultqueue, servers, logger):
        Thread.__init__(self)
        self.daemon = True
        self.logger = logger
        self.connection = connection
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.lock = lock
        self.servers = servers
        self.nntpobj = None
        self.running = True
        self.name, self.conn_nr = self.connection
        self.idn = self.name + " #" + str(self.conn_nr)
        self.bytesdownloaded = 0
        self.last_timestamp = 0
        self.mode = "download"
        self.download_done = True
        self.bandwidth_bytes = 0
        # 0 ... not running
        # 1 ... running ok
        # -1 ... connection problem
        self.connectionstate = 0

    def stop(self):
        self.running = False

    # return status, info
    #        status = 1:  ok
    #                 0:  article not found
    #                -1:  retention not sufficient
    #                -2:  server connection error
    def download_article(self, article_name, article_age):
        sn, _ = self.connection
        bytesdownloaded = 0
        info0 = None
        server_name, server_url, user, password, port, usessl, level, connections, retention = self.servers.get_single_server_config(sn)
        if self.mode == "sanitycheck":
            try:
                resp, number, message_id = self.nntpobj.stat(article_name)
                if article_name != message_id:
                    status = -1
                else:
                    status = 1
            except Exception as e:
                self.logger.error(whoami() + str(e) + self.idn + " for article " + article_name)
                status = -1
            return status, 0, 0
        if retention < article_age * 0.95:
            self.logger.warning(whoami() + "Retention on " + server_name + " not sufficient for article " + article_name)
            return -1, None
        try:
            # resp_h, info_h = self.nntpobj.head(article_name)
            resp, info = self.nntpobj.body(article_name)
            if resp.startswith("222"):
                status = 1
                info0 = [inf + b"\r\n" if not inf.endswith(b"\r\n") else inf for inf in info.lines]
                bytesdownloaded = sum(len(i) for i in info0)
            else:
                self.logger.warning(whoami() + resp + ": could not find " + article_name + " on " + self.idn)
                status = 0
        # nntpError 4xx - Command was syntactically correct but failed for some reason
        except nntplib.NNTPTemporaryError as e:
            errcode = e.response.strip()[:3]
            if errcode == "400":
                # server quits, new connection has to be established
                status = -2
            else:
                status = 0
            self.logger.warning(whoami() + e.response + ": could not find " + article_name + " on " + self.idn)
        # nntpError 5xx - Command unknown error
        except nntplib.NNTPPermanentError as e:
            errcode = e.response.strip()[:3]
            if errcode in ["503", "502"]:
                # timeout, closing connection
                status = -2
            else:
                status = 0
            self.logger.warning(whoami() + e.response + ": could not find " + article_name + " on " + self.idn)
            status = 0
        except nntplib.NNTPError as e:
            status = 0
            self.logger.warning(whoami() + e.response + ": could not find " + article_name + " on " + self.idn)
        except KeyboardInterrupt:
            status = -3
        except socket.timeout:
            status = -2
            self.logger.warning(whoami() + "socket.timeout on " + self.idn)
        except AttributeError as e:
            status = 0
            self.logger.warning(whoami() + str(e) + ": " + article_name + " on " + self.idn)
        except Exception as e:
            status = 0
            self.logger.warning(whoami() + str(e) + ": " + article_name + " on " + self.idn)
        self.bandwidth_bytes += bytesdownloaded
        return status, bytesdownloaded, info0

    def wait_running(self, sec):
        tt0 = time.time()
        while time.time() - tt0 < sec and self.running:
            time.sleep(0.1)
        return

    def retry_connect(self):
        if self.nntpobj or not self.running:
            self.connectionstate = 1
            return
        idx = 0
        name, conn_nr = self.connection
        idn = name + " #" + str(conn_nr)
        self.logger.debug(whoami() + "Server " + idn + " connecting ...")
        while idx < 5 and self.running:
            self.nntpobj = self.servers.open_connection(name, conn_nr)
            if self.nntpobj:
                self.logger.debug(whoami() + "Server " + idn + " connected!")
                self.last_timestamp = time.time()
                self.bytesdownloaded = 0
                self.bandwidth_bytes = 0
                self.connectionstate = 1
                self.wait_running(1)
                return
            self.logger.warning(whoami() + "Could not connect to server " + idn + ", will retry in 5 sec.")
            self.wait_running(5)
            if not self.running:
                break
            idx += 1
        if not self.running:
            self.logger.warning(whoami() + "No connection retries anymore due to exiting")
        else:
            self.logger.error(whoami() + "Connect retries to " + idn + " failed!")

    def remove_from_remaining_servers(self, name, remaining_servers):
        next_servers = []
        for s in remaining_servers:
            addserver = s[:]
            try:
                addserver.remove(name)
            except Exception as e:
                pass
            if addserver:
                next_servers.append(addserver)
        return next_servers

    def is_download_done(self):
        dld = None
        with self.lock:
            dld = self.download_done
        return dld

    def run(self):
        self.logger.info(whoami() + self.idn + " thread starting !")
        timeout = 2
        while self.running:
            with self.lock:
                self.download_done = True
            self.retry_connect()
            if not self.running:
                break
            if not self.nntpobj:
                self.wait_running(3)
                continue
            '''self.lock.acquire()
            artlist = list(self.articlequeue.queue)
            try:
                test_article = artlist[-1]
            except IndexError:
                self.lock.release()
                time.sleep(0.1)
                continue
            if not test_article:
                article = self.articlequeue.get()
                self.articlequeue.task_done()
                self.lock.release()
                self.logger.warning(self.idn + ": got poison pill!")
                break
            _, _, _, _, _, _, remaining_servers = test_article'''
            # articlequeue = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
            try:
                article = self.articlequeue.get_nowait()
                self.articlequeue.task_done()
            except (queue.Empty, EOFError):
                time.sleep(0.2)
                continue
            except Exception as e:
                self.logger.warning(whoami() + str(e) + ": problem in clearing article queue")
                time.sleep(0.2)
                continue
            # avoid ctrl-c to interrup downloading itself
            with self.lock:
                self.download_done = False
            filename, age, filetype, nr_articles, art_nr, art_name, remaining_servers1 = article
            if not remaining_servers1:
                self.resultqueue.put(article + (None,))
                continue
            if self.name not in remaining_servers1[0]:
                time.sleep(0.1)
                continue
            status, bytesdownloaded, info = self.download_article(art_name, age)
            # if ctrl-c - exit thread
            if status == -3 or not self.running:
                break
            # if download successfull - put to resultqueue
            elif status == 1:
                self.logger.debug(whoami() + "Downloaded article " + art_name + " on server " + self.idn)
                timeout = 2
                self.bytesdownloaded += bytesdownloaded
                self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, self.name, info, True))
                # self.articlequeue.task_done()
            # if 400 error
            elif status == -2:
                # disconnect
                self.logger.warning(whoami() + self.idn + " server connection error, reconnecting ...")
                self.connectionstate = -1
                try:
                    self.nntpobj.quit()
                except Exception as e:
                    pass
                self.nntpobj = None
                # take next server
                next_servers = self.remove_from_remaining_servers(self.name, remaining_servers1)
                next_servers.append([self.name])    # add current server to end of list
                self.logger.debug(whoami() + "Requeuing " + art_name + " on server " + self.idn)
                # requeue
                # self.articlequeue.task_done()
                self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
                self.wait_running(timeout)
                timeout *= 2
                if timeout > 30:
                    timeout = 2
                continue
            # if article could not be found on server / retention not good enough - requeue to other server
            elif status in [0, -1]:
                timeout = 2
                next_servers = self.remove_from_remaining_servers(self.name, remaining_servers1)
                # self.articlequeue.task_done()
                if not next_servers:
                    self.logger.error(whoami() + "Download finally failed on server " + self.idn + ": for article " + art_name + " " + str(next_servers))
                    self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, [], "failed", True))
                else:
                    self.logger.debug(whoami() + "Download failed on server " + self.idn + ": for article " + art_name + ", queueing: "
                                      + str(next_servers))
                    self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
        self.logger.info(whoami() + self.idn + " exited!")


# this class deals on a meta-level with usenet connections
class ConnectionThreads:
    def __init__(self, cfg, articlequeue, resultqueue, logger):
        self.cfg = cfg
        self.logger = logger
        self.threads = []
        self.lock = threading.Lock()
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.servers = None

    def init_servers(self):
        self.servers = Servers(self.cfg, self.logger)
        self.level_servers = self.servers.level_servers
        self.all_connections = self.servers.all_connections

    def start_threads(self):
        if not self.threads:
            self.logger.debug(whoami() + "starting download threads")
            self.init_servers()
            for sn, scon, _, _ in self.all_connections:
                t = ConnectionWorker(self.lock, (sn, scon), self.articlequeue, self.resultqueue, self.servers, self.logger)
                self.threads.append((t, time.time()))
                t.start()
        else:
            self.logger.debug(whoami() + "threads already started")

    def reset_timestamps(self):
        for t, _ in self.threads:
            t.last_timestamp = time.time()

    def reset_timestamps_bdl(self):
        if self.threads:
            for t, _ in self.threads:
                t.bytesdownloaded = 0
                t.last_timestamp = 0
                t.bandwidth_bytes = 0
                t.bandwidth_lasttt = 0
