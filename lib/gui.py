import psutil
import time
import sys
import datetime
import queue
import threading
from threading import Thread
import zmq
import os
import gi
import signal
gi.require_version('Gtk', '3.0')
from gi.repository import GLib, Gtk

MPP_MAIN = None


def closeall(a):
    Gtk.main_quit()
    if MPP_MAIN:
        os.kill(MPP_MAIN.pid, signal.SIGTERM)
        MPP_MAIN.join()


def app_main(mpp_main, logger):
    global MPP_MAIN
    MPP_MAIN = mpp_main
    win = Gtk.Window(default_height=50, default_width=900, title="ginzibix")
    win.connect("destroy", closeall)
    win.set_border_width(10)
    win.set_wmclass("ginzibix", "ginzibix")
    # win.set_icon_from_file("index.jpe")

    vbox = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=28)
    win.add(vbox)

    hbox = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
    vbox.add(hbox)

    nzb_label = Gtk.Label("Fuenf Freunde.nzb", xalign=0, yalign=0.5)
    hbox.pack_start(nzb_label, True, True, 0)

    progress = Gtk.ProgressBar(show_text=True)
    hbox.pack_start(progress, True, True, 0)

    info_label = Gtk.Label("Initializing ...", xalign=0, yalign=0.5)
    vbox.pack_start(info_label, True, True, 0)

    def update_mainwindow(data, pwdb_msg, server_config, threads, sortednzblist):
        # update progress bar of current NZB
        if data:
            bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
            gbdown0 = 0
            for t_bytesdownloaded, t_last_timestamp, t_idn in threads:
                gbdown = t_bytesdownloaded / (1024 * 1024 * 1024)
                gbdown0 += gbdown
            gbdown0 += already_downloaded_size
            fraction = gbdown0 / overall_size
            progress.set_fraction(fraction)
            progress.set_text(str(int(fraction * 100)) + "%")
            if nzbname:
                nzb_label.set_text(nzbname)
        # set info text
        try:
            msg0 = pwdb_msg[-1]
            if msg0:
                if len(msg0) > 120:
                    msg0 = msg0[:120] + "\n" + msg0[120:]
                info_label.set_text(msg0)
        except Exception:
            pass
        return False

    win.show_all()

    # start guiconnector
    lock = threading.Lock()
    guipoller = GUI_Poller(lock, update_mainwindow, logger, port="36601")
    guipoller.start()


# connects to GUI_Connector in main.py and gets data for displaying
class GUI_Poller(Thread):

    def __init__(self, lock, update_mainwindow, logger, port="36601"):
        Thread.__init__(self)
        self.daemon = True
        self.context = zmq.Context()
        self.host = "127.0.0.1"
        self.port = port
        self.lock = lock
        self.data = None
        self.nzbname = None
        self.delay = 1
        self.update_mainwindow = update_mainwindow

        self.socket = self.context.socket(zmq.REQ)
        self.logger = logger

    def run(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        socketurl = "tcp://" + self.host + ":" + self.port
        self.socket.connect(socketurl)
        # self.socket.RCVTIMEO = 1000
        sortednzblist = []
        while True:
            try:
                self.socket.send_string("REQ")
                datatype, datarec = self.socket.recv_pyobj()
                if datatype == "NOOK":
                    continue
                elif datatype == "DL_DATA":
                    data, pwdb_msg, server_config, threads = datarec
                elif datatype == "NZB_DATA":
                    sortednzblist = datarec
                try:
                    GLib.idle_add(self.update_mainwindow, data, pwdb_msg, server_config, threads, sortednzblist)
                except Exception:
                    pass
                # self.gui_drawer.draw(data, pwdb_msg, server_config, threads, sortednzblist)
            except Exception as e:
                self.logger.error("GUI_ConnectorMain: " + str(e))
            time.sleep(self.delay)


class GUI_Drawer:

    def draw(self, data, pwdb_msg, server_config, threads, sortednzblist):
        if not data or not pwdb_msg or not server_config or not threads:
            return
        bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
        avgmiblist = avgmiblist00
        max_mem_needed = 0
        bytescount0 = bytescount00
        bytescount0 += 0.00001
        overall_size += 0.00001
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
        for t_bytesdownloaded, t_last_timestamp, t_idn in threads:
            # for k, (t, last_timestamp) in enumerate(threads):
            gbdown = t_bytesdownloaded / (1024 * 1024 * 1024)
            gbdown0 += gbdown
            gbdown_str = "{0:.3f}".format(gbdown)
            mbitsec = (t_bytesdownloaded / (time.time() - t_last_timestamp)) / (1024 * 1024) * 8
            mbitsec0 += mbitsec
            mbitsec_str = "{0:.1f}".format(mbitsec)
            print(t_idn + ": Total - " + gbdown_str + " GB" + " | MBit/sec. - " + mbitsec_str + "                        ")
        print("-" * 60)
        gbdown0 += already_downloaded_size
        gbdown0_str = "{0:.3f}".format(gbdown0)
        perc0 = gbdown0 / overall_size
        if perc0 > 1:
            perc00 = 1
        else:
            perc00 = perc0
        print(gbdown0_str + " GiB (" + "{0:.1f}".format(perc00 * 100) + "%) of total "
              + "{0:.2f}".format(overall_size) + " GiB | MBit/sec. - " + "{0:.1f}".format(mbitsec0) + " " * 10)
        for key, item in filetypecounter00.items():
            print(key + ": " + str(item["counter"]) + "/" + str(item["max"]) + ", ", end="")
        if nzbname:
            trailing_spaces = " " * 10
        else:
            trailing_spaces = " " * 70
        print("Health: {0:.4f}".format(article_health * 100) + "%" + trailing_spaces)
        if mbitsec0 > 0 and perc0 < 1:
            eta = ((overall_size - gbdown0) * 1024)/(mbitsec0 / 8)
            print("Eta: " + str(datetime.timedelta(seconds=int(eta))) + " " * 30)
        else:
            print("Eta: - (done!)" + " " * 40)
        print()
        msg0 = pwdb_msg
        if msg0:
            print(" " * 120)
            sys.stdout.write("\033[F")
            print(msg0[-1][:120])
        # print nzblist
        lnz = 0
        for nzname, nzprio, nztimestamp, nzstatus, nzsize, nzdlsize in sortednzblist:
            if nzstatus in [0, 1, 2, 3]:
                
                nzsize /= (1024 * 1024 * 1024)
                nzdlsize /= (1024 * 1024 * 1024)
                if nzname == nzbname:
                    print(nzname + ": downloading " + " / " + gbdown0_str + " GiB / {0:.2f}".format(overall_size) + " GiB " + " " * 40)
                else:
                    print(nzname + ": " + str(nzstatus) + " / {0:.2f}".format(nzdlsize) + "GiB / {0:.2f}".format(nzsize) + " GiB" * 40)
                lnz += 1
        # go up on console
        for _ in range(len(threads) + 8 + lnz):
            sys.stdout.write("\033[F")
