import threading
import time
# import queue
from random import randint
import sys
import queue


def download(article, nntpob):
    r = randint(0, 99)
    # return True, "info!"
    if r <= 85:
        return True, "info!"
    else:
        return False, False


def connectionworker(connection):
    global resultarticles_dic, articlequeue, storedic
    name, nntpobj = connection
    idn = name + str(nntpobj)
    print(idn)
    while True:
        storedic[idn] = articlequeue.get()  # articlenr, remaining_servers = article
        if name not in storedic[idn][1]:
            articlequeue.task_done()            # ????????
            articlequeue.put(storedic[idn])
        else:
            print("Downloading on server " + name + ": + for article #" + str(storedic[idn][0]), storedic[idn][1])
            res, info = download(storedic[idn], nntpobj)
            if res:
                print("Download success on server " + name + ": for article #" + str(storedic[idn][0]), storedic[idn][1])
                resultarticles_dic[str(storedic[idn][0])] = info     # mit semaphore
                articlequeue.task_done()
            else:
                # storedic[idn][1].remove(name)
                # print(storedic[idn][1])
                storedic[idn] = (storedic[idn][0], [x for x in storedic[idn][1] if x != name])
                if not storedic[idn][1]:
                    print(">>>> Download finally failed on server " + name + ": for article #" + str(storedic[idn][0]), storedic[idn][1])
                    resultarticles_dic[str(storedic[idn][0])] = None     # mit semaphore
                    articlequeue.task_done()
                else:
                    articlequeue.task_done()    # ?????????????
                    articlequeue.put(storedic[idn])
                    print(">>>> Download failed on server " + name + ": for article #" + str(storedic[idn][0]), ", requeuing on servers:",
                          storedic[idn][1])
                    # print(">>> #" + str(storedic[idn][0]), [j for j in list(articlequeue.queue)])


# producer
def articleproducer(articles):
    global articlequeue
    # the main thread will put new articles to the queue
    for article in articles:
        articlequeue.put(article)
    # articlequeue.join()


# main
if __name__ == '__main__':
    nr_articles = 134
    storedic = {}
    articlequeue = queue.LifoQueue()
    articles0 = [str(i+1) for i in range(nr_articles)]
    resultarticles_dic = {key: None for key in articles0}

    all_connections = [("EWEKA", 1), ("EWEKA", 2), ("EWEKA", 3), ("EWEKA", 4), ("EWEKA", 5), ("EWEKA", 6),
                       ("BULK", 1), ("BULK", 2), ("BULK", 3), ("BULK", 4), ("BULK", 5), ("BULK", 6), ("BULK", 7),
                       ("TWEAK", 1), ("TWEAK", 2), ("TWEAK", 3), ("TWEAK", 4)]

    level_servers = {"0": ["EWEKA", "BULK"], "1": ["TWEAK"]}

    for level, serverlist in level_servers.items():
        level_servers = serverlist
        articles = [(key, level_servers) for key, item in resultarticles_dic.items() if not item]
        if not articles:
            print("All articles downloaded")
            break
        level_connections = [(name, connection) for name, connection in all_connections if name in level_servers]
        # consumer
        for c in level_connections:
            t = threading.Thread(target=connectionworker, args=(c, ))
            t.daemon = True
            t.start()
        # Produce
        t = threading.Thread(target=articleproducer, args=(articles, ))
        t.daemon = True
        t.start()
        articlequeue.join()
        print("------------------")
        l0 = len([item for key, item in resultarticles_dic.items()])
        l1 = len([item for key, item in resultarticles_dic.items() if item])
        print(l0, l1)
        print("Complete  Articles after level", level, " download:" + str(int(l1/l0) * 100))
        # print(resultarticles_dic)
