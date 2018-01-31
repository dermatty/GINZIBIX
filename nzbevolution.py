import threading
from threading import Thread
import time
# import queue
from random import randint
import sys
import queue

LOCK = threading.Lock()


def download(article, nntpob):
    r = randint(0, 99)
    # return True, "info!"
    if r <= 70:
        return True, "info!"
    else:
        return False, False


class ConnectionWorker(Thread):
    def __init__(self, lock, connection, articlequeue):
        Thread.__init__(self)
        self.connection = connection
        self.articlequeue = articlequeue
        self.lock = lock
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        global resultarticles_dic
        name, nntpobj = self.connection
        idn = name + str(nntpobj)
        print(idn + " started!")
        while self.running:
            # storedic[idn] = articlequeue.get()  # articlenr, remaining_servers = article
            try:
                storedic[idn] = self.articlequeue.get_nowait()  # articlenr, remaining_servers = article
                self.articlequeue.task_done()
            except Exception as e:
                continue
            if name not in storedic[idn][1]:
                # self.articlequeue.task_done()            # ????????
                self.articlequeue.put(storedic[idn])
            else:
                print("Downloading on server " + idn + ": + for article #" + str(storedic[idn][0]), storedic[idn][1])
                res, info = download(storedic[idn], nntpobj)
                # self.articlequeue.task_done()
                if res:
                    print("Download success on server " + idn + ": for article #" + str(storedic[idn][0]), storedic[idn][1])
                    with self.lock:
                        resultarticles_dic[str(storedic[idn][0])] = info     # mit semaphore
                    # self.articlequeue.task_done()
                else:
                    storedic[idn] = (storedic[idn][0], [x for x in storedic[idn][1] if x != name])
                    if not storedic[idn][1]:
                        print(">>>> Download finally failed on server " + idn + ": for article #" + str(storedic[idn][0]), storedic[idn][1])
                        with self.lock:
                            resultarticles_dic[str(storedic[idn][0])] = None     # mit semaphore
                        # self.articlequeue.task_done()
                    else:
                        # self.articlequeue.task_done()    # ?????????????
                        self.articlequeue.put(storedic[idn])
                        print(">>>> Download failed on server " + idn + ": for article #" + str(storedic[idn][0]), ", requeuing on servers:",
                              storedic[idn][1])
                        # print(">>> #" + str(storedic[idn][0]), [j for j in list(articlequeue.queue)])
        print(idn + " exited!")


def ArticleProducer(articles, articlequeue):
    for article in articles:
        articlequeue.put(article)


# main
if __name__ == '__main__':
    nr_articles = 134
    storedic = {}
    articlequeue = queue.Queue()
    articles0 = [str(i+1) for i in range(nr_articles)]
    resultarticles_dic = {key: None for key in articles0}

    all_connections = [("EWEKA", 1), ("EWEKA", 2), ("EWEKA", 3), ("EWEKA", 4), ("EWEKA", 5), ("EWEKA", 6),
                       ("BULK", 1), ("BULK", 2), ("BULK", 3), ("BULK", 4), ("BULK", 5), ("BULK", 6), ("BULK", 7),
                       ("TWEAK", 1), ("TWEAK", 2), ("TWEAK", 3), ("TWEAK", 4),
                       ("NEWS", 1), ("NEWS", 2), ("NEWS", 3), ("NEWS", 4), ("NEWS", 5), ("NEWS", 6), ("NEWS", 7),
                       ("BALD", 1), ("BALD", 2), ("BALD", 3), ("BALD", 4), ("BALD", 5), ("BALD", 6), ("BALD", 7)]

    level_servers0 = {"0": ["EWEKA", "BULK"], "1": ["TWEAK"], "2": ["NEWS", "BALD"]}

    t0 = time.time()
    for level, serverlist in level_servers0.items():
        level_servers = serverlist
        articles = [(key, level_servers) for key, item in resultarticles_dic.items() if not item]
        if not articles:
            print("All articles downloaded")
            break
        print("####", articles)
        level_connections = [(name, connection) for name, connection in all_connections if name in level_servers]
        if not level_connections:
            continue
        # Produce
        ArticleProducer(articles, articlequeue)
        # consumer
        threads = []
        for c in level_connections:
            t = ConnectionWorker(LOCK, c, articlequeue)
            threads.append(t)
            t.start()

        articlequeue.join()

        for t in threads:
            t.stop()
            t.join()

        print("Download failed:", [(key, item) for key, item in resultarticles_dic.items() if not item])
        l0 = len([item for key, item in resultarticles_dic.items()])
        l1 = len([item for key, item in resultarticles_dic.items() if item])
        # print(l0, l1)
        print("Complete  Articles after level", level, ": " + str(l1) + " out of " + str(l0))
        # print(resultarticles_dic)
        print("-" * 80)
    print(time.time() - t0)
