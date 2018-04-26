#!/home/stephan/.virtualenvs/nntp/bin/python

import os
import signal
import time
import psutil
import sys
from threading import Thread
import zmq
import ginzi
import logging
import logging.handlers
import multiprocessing as mp
import queue

lpref = __name__ + " - "


class Console_GUI_Thread(Thread):
    def __init__(self, guiqueue, logger):
        Thread.__init__(self)
        self.daemon = True
        self.running = False
        self.logger = logger
        self.guiqueue = guiqueue

    def display_console_connection_data(self, bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, server_config, threads):
        avgmiblist = avgmiblist00
        max_mem_needed = 0
        bytescount0 = bytescount00
        bytescount0 += 0.00001
        availmem0 = availmem00
        # get Mib downloaded
        if len(avgmiblist) > 50:
            del avgmiblist[0]
        if len(avgmiblist) > 10:
            avgmib_dic = {}
            for (server_name, _, _, _, _, _, _, _, _) in server_config:
                bytescountlist = [bytescount for (_, bytescount, download_server0) in avgmiblist if server_name == download_server0]
                if len(bytescountlist) > 2:
                    avgmib_db = sum(bytescountlist)
                    avgmib_mint = min([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    avgmib_maxt = max([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    # print(avgmib_maxt, avgmib_mint)
                    avgmib_dic[server_name] = (avgmib_db / (avgmib_maxt - avgmib_mint)) / (1024 * 1024) * 8
                else:
                    avgmib_dic[server_name] = 0
            for server_name, avgmib in avgmib_dic.items():
                mem_needed = ((psutil.virtual_memory()[0] - psutil.virtual_memory()[1]) - availmem0) / (1024 * 1024 * 1024)
                if mem_needed > max_mem_needed:
                    max_mem_needed = mem_needed
        # set in all threads servers alive timestamps
        # check if server is longer off > 120 sec, if yes, kill thread & stop server
        if nzbname:
            print("--> Downloading " + nzbname)
        else:
            print("--> Downloading paused!" + " " * 30)
        try:
            print("MBit/sec.: " + str([sn + ": " + str(int(av)) + "  " for sn, av in avgmib_dic.items()]) + " max. mem_needed: "
                  + "{0:.3f}".format(max_mem_needed) + " GB                 ")
        except UnboundLocalError:
            print("MBit/sec.: --- max. mem_needed: " + str(max_mem_needed) + " GB                ")
        gbdown0 = 0
        mbitsec0 = 0
        for k, (t, last_timestamp) in enumerate(threads):
            gbdown = t.bytesdownloaded / (1024 * 1024 * 1024)
            gbdown0 += gbdown
            gbdown_str = "{0:.3f}".format(gbdown)
            mbitsec = (t.bytesdownloaded / (time.time() - t.last_timestamp)) / (1024 * 1024) * 8
            mbitsec0 += mbitsec
            mbitsec_str = "{0:.1f}".format(mbitsec)
            print(t.idn + ": Total - " + gbdown_str + " GB" + " | MBit/sec. - " + mbitsec_str + "                        ")
        print("-" * 60)
        gbdown0_str = "{0:.3f}".format(gbdown0)
        print("Total GB: " + gbdown0_str + " = " + "{0:.1f}".format((gbdown0 / bytescount0) * 100) + "% of total "
              + "{0:.2f}".format(bytescount0) + "GB | MBit/sec. - " + "{0:.1f}".format(mbitsec0) + " " * 10)
        for key, item in filetypecounter00.items():
            print(key + ": " + str(item["counter"]) + "/" + str(item["max"]) + ", ", end="")
        if nzbname:
            trailing_spaces = " " * 10
        else:
            trailing_spaces = " " * 70
        print("Health: {0:.4f}".format(article_health * 100) + "%" + trailing_spaces)
        print()
        for _ in range(len(threads) + 6):
            sys.stdout.write("\033[F")

    def run(self):
        self.running = True
        self.logger.debug(lpref + "starting gui client thread")
        while self.running:
            guidata = None
            while True:
                try:
                    guidata = self.guiqueue.get_nowait()
                    # self.guiqueue.task_done()
                    break
                except queue.Empty:
                    pass
                time.sleep(0.1)
            if guidata:
                bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, server_config, threads = guidata
                self.display_console_connection_data(bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health,
                                                     server_config, threads)
            else:
                self.logger.debug(lpref + "gui client received none data!")
            time.sleep(0.1)


if __name__ == "__main__":

    guiqueue = mp.Queue()

    # init logger
    logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    progstr = "ginzibix 0.1-alpha, binary usenet downloader"
    print("Welcome to " + progstr)
    print("Press a key to quit")
    print("-" * 60)

    logger.info("-" * 80)
    logger.info("starting " + progstr)
    logger.info("-" * 80)

    t_gui_client = Console_GUI_Thread(guiqueue, logger)
    t_gui_client.start()

    mpp_ginziserver = mp.Process(target=ginzi.main, args=(guiqueue, logger, ))
    mpp_ginziserver.start()
    mpp_ginziserver_pid = mpp_ginziserver.pid

    ch = input()

    # t_gui_client.running = False
    # t_gui_client.join()
    os.kill(mpp_ginziserver_pid, signal.SIGTERM)
    
