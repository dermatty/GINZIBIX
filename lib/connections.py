from threading import Thread
import nntplib
import time

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
        server_name, server_url, user, password, port, usessl, level, connections, retention = self.servers.get_single_server_config(sn)
        if self.mode == "sanitycheck":
            try:
                resp, number, message_id = self.nntpobj.stat(article_name)
                if article_name != message_id:
                    status = -1
                else:
                    status = 1
            except Exception as e:
                self.logger.error(lpref + str(e) + self.idn + " for article " + article_name)
                status = -1
            return status, 0, 0
        if retention < article_age * 0.95:
            self.logger.warning(lpref + "Retention on " + server_name + " not sufficient for article " + article_name)
            return -1, None
        try:
            # resp_h, info_h = self.nntpobj.head(article_name)
            resp, info = self.nntpobj.body(article_name)
            if resp[:3] != "222":
                # if resp_h[:3] != "221" or resp[:3] != "222":
                self.logger.warning(lpref + "Could not find " + article_name + " on " + self.idn)
                status = 0
                info0 = None
            else:
                status = 1
                info0 = [inf for inf in info.lines]
                bytesdownloaded = sum(len(i) for i in info0)
        except nntplib.NNTPTemporaryError:
            self.logger.warning(lpref + "Could not find " + article_name + " on " + self.idn)
            status = 0
            info0 = None
        except KeyboardInterrupt:
            status = -3
            info0 = None
        except Exception as e:
            self.logger.error(lpref + str(e) + self.idn + " for article " + article_name)
            status = -2
            info0 = None
        self.bandwidth_bytes += bytesdownloaded
        return status, bytesdownloaded, info0

    def retry_connect(self):
        if self.nntpobj or not self.running:
            self.connectionstate = 1
            return
        idx = 0
        name, conn_nr = self.connection
        idn = name + " #" + str(conn_nr)
        self.logger.debug(lpref + "Server " + idn + " connecting ...")
        while idx < 5 and self.running:
            self.nntpobj = self.servers.open_connection(name, conn_nr)
            if self.nntpobj:
                self.logger.debug(lpref + "Server " + idn + " connected!")
                self.last_timestamp = time.time()
                self.bytesdownloaded = 0
                self.bandwidth_bytes = 0
                self.connectionstate = 1
                time.sleep(1)
                return
            self.logger.warning(lpref + "Could not connect to server " + idn + ", will retry in 5 sec.")
            time.sleep(5)
            idx += 1
        if not self.running:
            self.logger.warning(lpref + "No connection retries anymore due to exiting")
        else:
            self.logger.error(lpref + "Connect retries to " + idn + " failed!")

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
        self.logger.info(lpref + self.idn + " thread starting !")
        timeout = 5
        while True and self.running:
            with self.lock:
                self.download_done = True
            self.retry_connect()
            if not self.nntpobj:
                time.sleep(5)
                continue
            self.lock.acquire()
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
            _, _, _, _, _, _, remaining_servers = test_article
            # no servers left
            # articlequeue = (filename, age, filetype, nr_articles, art_nr, art_name, level_servers)
            if not remaining_servers:
                article = self.articlequeue.get()
                self.lock.release()
                self.resultqueue.put(article + (None,))
                continue
            if self.name not in remaining_servers[0]:
                self.lock.release()
                time.sleep(0.1)
                continue
            article = self.articlequeue.get()
            self.lock.release()
            filename, age, filetype, nr_articles, art_nr, art_name, remaining_servers1 = article
            # avoid ctrl-c to interrup downloading itself
            with self.lock:
                self.download_done = False
            status, bytesdownloaded, info = self.download_article(art_name, age)
            # if ctrl-c - exit thread
            if status == -3:
                break
            # if server connection error - disconnect
            if status == -2:
                # disconnect
                self.logger.warning("Stopping server " + self.idn)
                self.connectionstate = -1
                try:
                    self.nntpobj.quit()
                except Exception as e:
                    pass
                self.nntpobj = None
                # take next server
                next_servers = self.remove_from_remaining_servers(self.name, remaining_servers)
                next_servers.append([self.name])    # add current server to end of list
                self.logger.warning(lpref + "Requeuing " + art_name + " on server " + self.idn)
                # requeue
                self.articlequeue.task_done()
                self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
                time.sleep(timeout)
                timeout += 3
                if timeout > 30:
                    timeout = 5
                continue
            # if download successfull - put to resultqueue
            if status == 1:
                self.bytesdownloaded += bytesdownloaded
                # print("Download success on server " + idn + ": for article #" + str(art_nr), filename)
                self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, self.name, info, True))
                self.articlequeue.task_done()
            # if article could not be found on server / retention not good enough - requeue to other server
            if status in [0, -1]:
                next_servers = self.remove_from_remaining_servers(self.name, remaining_servers)
                self.articlequeue.task_done()
                if not next_servers:
                    self.logger.error(lpref + "Download finally failed on server " + self.idn + ": for article #" + str(art_nr) + " " + str(next_servers))
                    self.resultqueue.put((filename, age, filetype, nr_articles, art_nr, art_name, [], "failed", True))
                else:
                    self.logger.warning(lpref + "Download failed on server " + self.idn + ": for article #" + str(art_nr) + ", queueing: " + str(next_servers))
                    self.articlequeue.put((filename, age, filetype, nr_articles, art_nr, art_name, next_servers))
        self.logger.debug(lpref + self.idn + " exited!")
