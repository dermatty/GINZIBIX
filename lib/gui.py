import gi
import queue
import configparser
import threading
import time
import datetime
from gi.repository import Gtk, Gio, GdkPixbuf, GLib
from .aux import get_cut_nzbname, get_cut_msg, get_bg_color, GUI_Poller
from .mplogging import whoami, setup_logger

gi.require_version("Gtk", "3.0")

__appname__ = "ginzibix"
__version__ = "0.01 pre-alpha"
__author__ = "dermatty"

ICONFILE = "lib/gzbx1.png"
GLADEFILE = "lib/ginzibix.glade"

CLOSEALL_TIMEOUT = 1
MAX_NZB_LEN = 50
MAX_MSG_LEN = 120

MENU_XML = """
<?xml version="1.0" encoding="UTF-8"?>
<interface>
  <menu id="app-menu">
    <section>
      <item>
        <attribute name="action">app.settings</attribute>
        <attribute name="label" translatable="yes">_Settings</attribute>
      </item>
      <item>
        <attribute name="action">app.about</attribute>
        <attribute name="label" translatable="yes">_About</attribute>
      </item>
      <item>
        <attribute name="action">app.quit</attribute>
        <attribute name="label" translatable="yes">_Quit</attribute>
        <attribute name="accel">&lt;Primary&gt;q</attribute>
    </item>
    </section>
  </menu>
</interface>
"""


# ******************************************************************************************************
# *** main gui itself
class ApplicationGui(Gtk.Application):

    def __init__(self, dirs, cfg_file, mp_loggerqueue):

        # init app
        self.app = Gtk.Application.new("org.dermatty.ginzibix", Gio.ApplicationFlags(0), )
        self.app.connect("startup", self.on_app_startup)
        self.app.connect("activate", self.on_app_activate)
        self.app.connect("shutdown", self.on_app_shutdown)

        # data
        self.logger = setup_logger(mp_loggerqueue, __file__)
        self.cfg_file = cfg_file
        self.cfg = configparser.ConfigParser()
        self.dirs = dirs
        self.lock = threading.Lock()
        self.liststore = None
        self.liststore_s = None
        self.liststore_nzbhistory = None
        self.mbitlabel2 = None
        self.single_selected = None
        try:
            self.cfg.read(cfg_file)
        except Exception as e:
            self.logger.error(whoami() + str(e) + ", exiting!")
            self.app.quit()
            # Gtk.main_quit() ???
        self.appdata = AppData(self.lock)
        self.read_config_file()
        self.dl_running = True
        self.nzb_status_string = ""
        self.guiqueue = queue.Queue()
        self.lock = threading.Lock()

    def run(self, argv):
        self.app.run(argv)

    def on_app_activate(self, app):
        print("app_activate")
        self.builder = Gtk.Builder()
        self.builder.add_from_file(GLADEFILE)
        self.builder.add_from_string(MENU_XML)
        app.set_app_menu(self.builder.get_object("app-menu"))

        self.obj = self.builder.get_object
        self.window = self.obj("main_window")
        self.levelbar = self.obj("levelbar")
        self.mbitlabel2 = self.obj("mbitlabel2")
        self.no_queued_label = self.obj("no_queued")
        self.overall_eta_label = self.obj("overalleta")
        self.builder.connect_signals(Handler(self))
        self.window.set_application(self.app)

        # set icon & app name
        try:
            self.window.set_icon_from_file(ICONFILE)
        except GLib.GError as e:
            self.logger.error(whoami() + str(e) + "cannot find icon file!" + ICONFILE)
        self.window.set_wmclass(__appname__, __appname__)

        # set app position
        screen = self.window.get_screen()
        self.max_width = screen.get_width()
        self.max_height = screen.get_height()
        self.n_monitors = screen.get_n_monitors()
        self.window.set_default_size(int(self.max_width/(2 * self.n_monitors)), self.max_height)
        self.window.set_position(Gtk.WindowPosition.CENTER_ALWAYS)

        # add non-standard actions
        self.add_simple_action("about", self.on_action_about_activated)
        self.add_simple_action("message", self.on_action_message_activated)
        self.add_simple_action("quit", self.on_action_quit_activated)
        self.add_simple_action("settings", self.on_action_settings_activated)

        # set liststore for nzb downloading list
        self.setup_frame_nzblist()
        self.setup_frame_logs()
        self.setup_frame_nzbhistory()

        self.gridbuttonlist = [self.obj("button_top"), self.obj("button_up"), self.obj("button_down"),
                               self.obj("button_bottom"), self.obj("button_interrupt"), self.obj("button_delete")]
        self.window.show_all()

        self.guipoller = GUI_Poller(self, delay=self.update_delay, host=self.host, port=self.port)
        self.guipoller.start()

        # call main thread from here!!

    def on_app_startup(self, app):
        pass

    def on_app_shutdown(self, app):
        self.appdata.closeall = True
        self.guiqueue.put(("closeall", None))
        t0 = time.time()
        while self.appdata.closeall and time.time() - t0 < CLOSEALL_TIMEOUT:
            time.sleep(0.1)
        self.guipoller.stop()

    # ----------------------------------------------------------------------------------------------------
    # -- Application menu actions

    def add_simple_action(self, name, callback):
        action = Gio.SimpleAction.new(name)
        action.connect("activate", callback)
        self.app.add_action(action)

    def on_action_settings_activated(self, action, param):
        pass

    def on_action_about_activated(self, action, param):
        about_dialog = Gtk.AboutDialog(transient_for=self.window, modal=True)
        about_dialog.set_program_name(__appname__)
        about_dialog.set_version(__version__)
        about_dialog.set_copyright("Copyright \xa9 2018 dermatty")
        about_dialog.set_comments("A binary newsreader for the Gnome desktop")
        about_dialog.set_website("https://github.com/dermatty/GINZIBIX")
        about_dialog.set_website_label('Ginzibix on GitHub')
        try:
            about_dialog.set_logo(GdkPixbuf.Pixbuf.new_from_file_at_size(ICONFILE, 64, 64))
        except GLib.GError as e:
            print(whoami() + str(e) + "cannot find icon file!")

        about_dialog.set_license_type(Gtk.License.GPL_3_0)
        about_dialog.present()

    def on_action_message_activated(self, actiong, param):
        pass

    def on_action_quit_activated(self, action, param):
        self.app.quit()

    # ----------------------------------------------------------------------------------------------------
    #  -- liststores / treeviews

    # liststore/treeview for logs in stack DOWNLOADING
    def setup_frame_logs(self):
        self.logs_liststore = Gtk.ListStore(str, str, str, str, str)
        self.treeview_loglist = Gtk.TreeView(model=self.logs_liststore)

        renderer_log4 = Gtk.CellRendererText()
        column_log4 = Gtk.TreeViewColumn("Time", renderer_log4, text=2, background=3, foreground=4)
        column_log4.set_min_width(80)
        self.treeview_loglist.append_column(column_log4)

        renderer_log3 = Gtk.CellRendererText()
        column_log3 = Gtk.TreeViewColumn("Level", renderer_log3, text=1, background=3, foreground=4)
        column_log3.set_min_width(80)
        self.treeview_loglist.append_column(column_log3)

        renderer_log2 = Gtk.CellRendererText()
        column_log2 = Gtk.TreeViewColumn("Message", renderer_log2, text=0, background=3, foreground=4)
        column_log2.set_expand(True)
        column_log2.set_min_width(520)
        self.treeview_loglist.append_column(column_log2)

        self.obj("scrolled_window_logs").add(self.treeview_loglist)

    # liststore/treeview for nzbhistory
    def setup_frame_nzbhistory(self):
        self.nzbhistory_liststore = Gtk.ListStore(str, int)
        self.update_nzbhistory_liststore()
        self.treeview_history = Gtk.TreeView(model=self.nzbhistory_liststore)
        self.treeview_history.set_reorderable(False)
        sel = self.treeview_history.get_selection()
        sel.set_mode(Gtk.SelectionMode.NONE)
        # 0st column: NZB name
        detailsrenderer_text0 = Gtk.CellRendererText()
        detailscolumn_text0 = Gtk.TreeViewColumn("NZB name", detailsrenderer_text0, text=0)
        detailscolumn_text0.set_expand(True)
        detailscolumn_text0.set_min_width(320)
        self.treeview_history.append_column(detailscolumn_text0)
        # 1th column status
        detailsrenderer_text1 = Gtk.CellRendererText()
        detailscolumn_text1 = Gtk.TreeViewColumn("Status", detailsrenderer_text1, text=1)
        detailscolumn_text1.set_min_width(80)
        self.treeview_history.append_column(detailscolumn_text1)
        self.obj("detailsscrolled_window").add(self.treeview_history)

    # liststore/treeview for nzblist in stack DOWNLOADING
    def setup_frame_nzblist(self):
        self.nzblist_liststore = Gtk.ListStore(str, int, float, float, str, str, bool, str, str)
        self.update_liststore()
        # set treeview + actions
        self.treeview_nzblist = Gtk.TreeView(model=self.nzblist_liststore)
        self.treeview_nzblist.set_reorderable(False)
        sel = self.treeview_nzblist.get_selection()
        sel.set_mode(Gtk.SelectionMode.NONE)
        # 0th selection toggled
        renderer_toggle = Gtk.CellRendererToggle()
        column_toggle = Gtk.TreeViewColumn("Select", renderer_toggle, active=6)
        renderer_toggle.connect("toggled", self.on_inverted_toggled)
        self.treeview_nzblist.append_column(column_toggle)
        # 1st column: NZB name
        renderer_text0 = Gtk.CellRendererText()
        column_text0 = Gtk.TreeViewColumn("NZB name", renderer_text0, text=0)
        column_text0.set_expand(True)
        column_text0.set_min_width(320)
        self.treeview_nzblist.append_column(column_text0)
        # 2nd: progressbar
        renderer_progress = Gtk.CellRendererProgress()
        column_progress = Gtk.TreeViewColumn("Progress", renderer_progress, value=1, text=5)
        column_progress.set_min_width(260)
        column_progress.set_expand(True)
        self.treeview_nzblist.append_column(column_progress)
        # 3rd downloaded GiN
        renderer_text1 = Gtk.CellRendererText()
        column_text1 = Gtk.TreeViewColumn("Downloaded", renderer_text1, text=2)
        column_text1.set_cell_data_func(renderer_text1, lambda col, cell, model, iter, unused:
                                        cell.set_property("text", "{0:.2f}".format(model.get(iter, 2)[0]) + " GiB"))
        self.treeview_nzblist.append_column(column_text1)
        # 4th overall GiB
        renderer_text2 = Gtk.CellRendererText()
        column_text2 = Gtk.TreeViewColumn("Overall", renderer_text2, text=3)
        column_text2.set_cell_data_func(renderer_text2, lambda col, cell, model, iter, unused:
                                        cell.set_property("text", "{0:.2f}".format(model.get(iter, 3)[0]) + " GiB"))
        column_text2.set_min_width(80)
        self.treeview_nzblist.append_column(column_text2)
        # 5th Eta
        renderer_text3 = Gtk.CellRendererText()
        column_text3 = Gtk.TreeViewColumn("Eta", renderer_text3, text=4)
        column_text3.set_min_width(80)
        self.treeview_nzblist.append_column(column_text3)
        # 7th status
        renderer_text7 = Gtk.CellRendererText()
        column_text7 = Gtk.TreeViewColumn("Status", renderer_text7, text=7, background=8)
        column_text7.set_min_width(80)
        self.treeview_nzblist.append_column(column_text7)
        self.obj("scrolled_window_nzblist").add(self.treeview_nzblist)

    # ----------------------------------------------------------------------------------------------------
    # all the gui update functions

    def update_mainwindow(self, data, server_config, threads, dl_running, nzb_status_string, netstat_mbitcur, sortednzblist0,
                          sortednzbhistorylist0, article_health, connection_health, dlconfig, fulldata):

        # fulldata: contains messages
        if fulldata and self.appdata.fulldata != fulldata:
            self.appdata.fulldata = fulldata
            try:
                self.appdata.nzbname = fulldata["all#"][0]
            except Exception as e:
                self.appdata.nzbname = None
            self.update_logstore()

        # nzbhistory
        if sortednzbhistorylist0 and sortednzbhistorylist0 != self.appdata.sortednzbhistorylist:
            if sortednzbhistorylist0 == [-1]:
                sortednzbhistorylist = []
            else:
                sortednzbhistorylist = sortednzbhistorylist0
            nzbs_copy = self.appdata.nzbs_history.copy()
            self.appdata.nzbs_history = []
            for idx1, (n_name, n_prio, n_updatedate, n_status, n_siz, n_downloaded) in enumerate(sortednzbhistorylist):
                self.appdata.nzbs_history.append((n_name, n_status))
            if nzbs_copy != self.appdata.nzbs_history:
                self.update_nzbhistory_liststore()
            self.appdata.sortednzbhistorylist = sortednzbhistorylist0[:]

        # downloading nzbs
        if (sortednzblist0 and sortednzblist0 != self.appdata.sortednzblist):    # or (sortednzblist0 == [-1] and self.appdata.sortednzblist):
            # sort again just to make sure
            if sortednzblist0 == [-1]:
                sortednzblist = []
            else:
                sortednzblist = sorted(sortednzblist0, key=lambda prio: prio[1])
            gibdivisor = (1024 * 1024 * 1024)
            nzbs_copy = self.appdata.nzbs.copy()
            self.appdata.nzbs = []
            for idx1, (n_name, n_prio, n_ts, n_status, n_siz, n_downloaded) in enumerate(sortednzblist):
                try:
                    n_perc = min(int((n_downloaded/n_siz) * 100), 100)
                except ZeroDivisionError:
                    n_perc = 0
                n_dl = n_downloaded / gibdivisor
                n_size = n_siz / gibdivisor
                if self.appdata.mbitsec > 0 and self.dl_running:
                    eta0 = (((n_size - n_dl) * 1024) / (self.appdata.mbitsec / 8))
                    if eta0 < 0:
                        eta0 = 0
                    try:
                        etastr = str(datetime.timedelta(seconds=int(eta0)))
                    except Exception as e:
                        etastr = "-"
                else:
                    etastr = "-"
                selected = False
                for n_name0, n_perc0, n_dl0, n_size0, etastr0, n_percstr0, selected0, status0 in nzbs_copy:
                    if n_name0 == n_name:
                        selected = selected0
                self.appdata.nzbs.append((n_name, n_perc, n_dl, n_size, etastr, str(n_perc) + "%", selected, n_status))
            if nzbs_copy != self.appdata.nzbs:
                self.update_liststore()
                # self.update_liststore_dldata()
            self.appdata.sortednzblist = sortednzblist0[:]

        if data and None not in data:   # and (data != self.appdata.dldata or netstat_mbitcur != self.appdata.netstat_mbitcur):
            bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
            try:
                firstsortednzb = sortednzblist0[0][0]
            except Exception:
                firstsortednzb = None
            if nzbname is not None and nzbname == firstsortednzb:
                mbitseccurr = 0
                # calc gbdown, mbitsec_avg
                gbdown0 = 0
                for t_bytesdownloaded, t_last_timestamp, t_idn, t_bandwbytes in threads:
                    gbdown = t_bytesdownloaded / (1024 * 1024 * 1024)
                    gbdown0 += gbdown
                gbdown0 += already_downloaded_size
                mbitseccurr = netstat_mbitcur
                if not dl_running:
                    self.dl_running = False
                else:
                    self.dl_running = True
                self.nzb_status_string = nzb_status_string
                with self.lock:
                    if mbitseccurr > self.appdata.max_mbitsec:
                        self.appdata.max_mbitsec = mbitseccurr
                        self.levelbar.set_tooltip_text("Max = " + str(int(self.appdata.max_mbitsec)))
                    self.appdata.nzbname = nzbname
                    if nzb_status_string == "postprocessing" or nzb_status_string == "success":
                        self.appdata.overall_size = overall_size
                        self.appdata.gbdown = overall_size
                        self.appdata.mbitsec = 0
                    else:
                        self.appdata.overall_size = overall_size
                        self.appdata.gbdown = gbdown0
                        self.appdata.mbitsec = mbitseccurr
                    self.update_liststore_dldata()
                    self.update_liststore(only_eta=True)
                    self.appdata.dldata = data
        return False

    def update_first_appdata_nzb(self):
        if self.appdata.nzbs:
            _, _, n_dl, n_size, _, _, _, _ = self.appdata.nzbs[0]
            self.appdata.gbdown = n_dl
            self.appdata.overall_size = n_size

    def read_config_file(self):
        # update_delay
        try:
            self.update_delay = float(self.cfg["GTKGUI"]["UPDATE_DELAY"])
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ", setting update_delay to default 0.5")
            self.update_delay = 0.5
        # host ip
        try:
            self.host = self.cfg["OPTIONS"]["HOST"]
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ", setting host to default 127.0.0.1")
            self.host = "127.0.0.1"
        # host port
        try:
            self.port = self.cfg["OPTIONS"]["PORT"]
            assert(int(self.port) > 1024 and int(self.port) <= 65535)
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ", setting port to default 36603")
            self.port = "36603"

    def toggle_buttons(self):
        one_is_selected = False
        if not one_is_selected:
            for ls in range(len(self.nzblist_liststore)):
                path0 = Gtk.TreePath(ls)
                if self.nzblist_liststore[path0][6]:
                    one_is_selected = True
                    break
        for b in self.gridbuttonlist:
            if one_is_selected:
                b.set_sensitive(True)
            else:
                b.set_sensitive(False)
        return False    # because of Glib.idle_add

    def set_buttons_insensitive(self):
        for b in self.gridbuttonlist:
            b.set_sensitive(False)

    def remove_selected_from_list(self):
        old_first_nzb = self.appdata.nzbs[0]
        newnzbs = []
        for i, ro in enumerate(self.nzblist_liststore):
            if not ro[6]:
                newnzbs.append(self.appdata.nzbs[i])
        self.appdata.nzbs = newnzbs[:]
        if self.appdata.nzbs:
            if self.appdata.nzbs[0] != old_first_nzb:
                self.update_first_appdata_nzb()

    def on_inverted_toggled(self, widget, path):
        with self.lock:
            self.nzblist_liststore[path][6] = not self.nzblist_liststore[path][6]
            i = int(path)
            newnzb = list(self.appdata.nzbs[i])
            newnzb[6] = self.nzblist_liststore[path][6]
            self.appdata.nzbs[i] = tuple(newnzb)
            self.toggle_buttons()

    def update_logstore(self):
        # only show msgs for current nzb
        self.logs_liststore.clear()
        if not self.appdata.nzbname:
            return
        try:
            loglist = self.appdata.fulldata[self.appdata.nzbname]["msg"][:]
        except Exception:
            return
        for msg0, ts0, level0 in loglist:
            log_as_list = []
            # msg, level, tt, bg, fg
            log_as_list.append(get_cut_msg(msg0, MAX_MSG_LEN))
            log_as_list.append(level0)
            if level0 == 0:
                log_as_list.append("")
            else:
                log_as_list.append(str(datetime.datetime.fromtimestamp(ts0).strftime('%Y-%m-%d %H:%M:%S')))
            fg = "black"
            if log_as_list[1] == "info":
                bg = "royal Blue"
                fg = "white"
            elif log_as_list[1] == "warning":
                bg = "orange"
                fg = "white"
            elif log_as_list[1] == "error":
                bg = "red"
                fg = "white"
            elif log_as_list[1] == "success":
                bg = "green"
                fg = "white"
            else:
                bg = "white"
            log_as_list.append(bg)
            log_as_list.append(fg)
            self.logs_liststore.append(log_as_list)

    def update_nzbhistory_liststore(self):
        self.nzbhistory_liststore.clear()
        for i, nzb in enumerate(self.appdata.nzbs_history):
            nzb_as_list = list(nzb)
            self.nzbhistory_liststore.append(nzb_as_list)

    def update_liststore(self, only_eta=False):
        # n_name, n_perc, n_dl, n_size, etastr, str(n_perc) + "%", selected, n_status))
        if only_eta:
            self.appdata.overall_eta = 0
            for i, nzb in enumerate(self.appdata.nzbs):
                # skip first one as it will be updated anyway
                if i == 0:
                    continue
                try:
                    path = Gtk.TreePath(i)
                    iter = self.nzblist_liststore.get_iter(path)
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
                    continue
                eta0 = 0
                if self.appdata.mbitsec > 0 and self.dl_running:
                    overall_size = nzb[3]
                    gbdown = nzb[2]
                    eta0 = (((overall_size - gbdown) * 1024) / (self.appdata.mbitsec / 8))
                    if eta0 < 0:
                        eta0 = 0
                    try:
                        etastr = str(datetime.timedelta(seconds=int(eta0)))
                    except Exception as e:
                        etastr = "-"
                else:
                    etastr = "-"
                self.appdata.overall_eta += eta0
                self.nzblist_liststore.set_value(iter, 4, etastr)
            return

        self.nzblist_liststore.clear()
        for i, nzb in enumerate(self.appdata.nzbs):
            nzb_as_list = list(nzb)
            nzb_as_list[0] = get_cut_nzbname(nzb_as_list[0], MAX_NZB_LEN)
            n_status = nzb_as_list[7]
            if n_status == 0:
                n_status_s = "preprocessing"
            elif n_status == 1:
                n_status_s = "queued"
            elif n_status == 2:
                n_status_s = "downloading"
            elif n_status == 3:
                n_status_s = "postprocessing"
            elif n_status == 4:
                n_status_s = "success"
            elif n_status < 0:
                n_status_s = "failed"
            else:
                n_status_s = "unknown"
            # only set bgcolor for first row
            if i != 0:
                n_status_s = "idle(" + n_status_s + ")"
                bgcolor = "white"
            else:
                if not self.appdata.dl_running:
                    n_status_s = "paused"
                    bgcolor = "white"
                else:
                    bgcolor = get_bg_color(n_status_s)
            nzb_as_list[7] = n_status_s
            nzb_as_list.append(bgcolor)
            self.nzblist_liststore.append(nzb_as_list)

    def update_liststore_dldata(self):
        if len(self.nzblist_liststore) == 0:
            self.levelbar.set_value(0)
            self.mbitlabel2.set_text("- Mbit/s")
            self.no_queued_label.set_text("Queued: -")
            self.overall_eta_label.set_text("ETA: -")
            return
        path = Gtk.TreePath(0)
        iter = self.nzblist_liststore.get_iter(path)

        if self.appdata.overall_size > 0:
            n_perc = min(int((self.appdata.gbdown / self.appdata.overall_size) * 100), 100)
        else:
            n_perc = 0
        n_dl = self.appdata.gbdown
        n_size = self.appdata.overall_size

        if not self.appdata.dl_running:
            self.nzb_status_string = "paused"
            n_bgcolor = "white"
        else:
            n_bgcolor = get_bg_color(self.nzb_status_string)

        self.nzblist_liststore.set_value(iter, 1, n_perc)
        self.nzblist_liststore.set_value(iter, 2, n_dl)
        self.nzblist_liststore.set_value(iter, 3, n_size)
        self.nzblist_liststore.set_value(iter, 5, str(n_perc) + "%")
        self.nzblist_liststore.set_value(iter, 7, self.nzb_status_string)
        self.nzblist_liststore.set_value(iter, 8, n_bgcolor)
        etastr = "-"
        overall_etastr = str(datetime.timedelta(seconds=int(self.appdata.overall_eta)))
        try:
            if self.appdata.mbitsec > 0 and self.dl_running:
                eta0 = (((self.appdata.overall_size - self.appdata.gbdown) * 1024) / (self.appdata.mbitsec / 8))
                if eta0 < 0:
                    eta0 = 0
                self.appdata.overall_eta += eta0
                overall_etastr = str(datetime.timedelta(seconds=int(self.appdata.overall_eta)))
                etastr = str(datetime.timedelta(seconds=int(eta0)))
        except Exception:
            pass
        self.nzblist_liststore.set_value(iter, 4, etastr)
        if len(self.appdata.nzbs) > 0:
            newnzb = (self.appdata.nzbs[0][0], n_perc, n_dl, n_size, etastr, str(n_perc) + "%", self.appdata.nzbs[0][6], self.appdata.nzbs[0][7])
            self.no_queued_label.set_text("Queued: " + str(len(self.appdata.nzbs)))
            self.appdata.nzbs[0] = newnzb
            if self.appdata.mbitsec > 0 and self.appdata.dl_running:
                self.levelbar.set_value(self.appdata.mbitsec / self.appdata.max_mbitsec)
                mbitsecstr = str(int(self.appdata.mbitsec)) + " MBit/s"
                self.mbitlabel2.set_text(mbitsecstr.rjust(11))
                self.overall_eta_label.set_text("ETA: " + overall_etastr)
            else:
                self.levelbar.set_value(0)
                self.mbitlabel2.set_text("- MBit/s")
                self.overall_eta_label.set_text("ETA: -")
        else:
            self.levelbar.set_value(0)
            self.mbitlabel2.set_text("")
            self.no_queued_label.set_text("Queued: -")
            self.overall_eta_label.set_text("ETA: -")


# ******************************************************************************************************
# *** Handler for buttons

class Handler:

    def __init__(self, gui):
        self.gui = gui

    def on_button_pause_resume_clicked(self, button):
        with self.gui.lock:
            self.gui.appdata.dl_running = not self.gui.appdata.dl_running
        self.gui.guiqueue.put(("dl_running", self.gui.appdata.dl_running))
        if self.gui.appdata.dl_running:
            icon = Gio.ThemedIcon(name="media-playback-pause")
            image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
            button.set_image(image)
        else:
            icon = Gio.ThemedIcon(name="media-playback-start")
            image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
            button.set_image(image)
        self.gui.update_liststore_dldata()
        self.gui.update_liststore()
        print("pause resume clicked!")

    def on_button_settings_clicked(self, button):
        print("settings clicked")

    def on_button_top_clicked(self, button):
        with self.gui.lock:
            old_first_nzb = self.gui.appdata.nzbs[0]
            newnzbs = []
            for i, ro in enumerate(self.gui.nzblist_liststore):
                if ro[6]:
                    newnzbs.append(self.gui.appdata.nzbs[i])
            for i, ro in enumerate(self.gui.nzblist_liststore):
                if not ro[6]:
                    newnzbs.append(self.gui.appdata.nzbs[i])
            self.gui.appdata.nzbs = newnzbs[:]
            if self.gui.appdata.nzbs[0] != old_first_nzb:
                self.gui.update_first_appdata_nzb()
            self.gui.update_liststore()
            self.gui.update_liststore_dldata()
            self.gui.set_buttons_insensitive()
            self.gui.guiqueue.put(("order_changed", None))
        print("full up clicked")

    def on_button_bottom_clicked(self, button):
        with self.gui.lock:
            old_first_nzb = self.gui.appdata.nzbs[0]
            newnzbs = []
            for i, ro in enumerate(self.gui.nzblist_liststore):
                if not ro[6]:
                    newnzbs.append(self.gui.appdata.nzbs[i])
            for i, ro in enumerate(self.gui.nzblist_liststore):
                if ro[6]:
                    newnzbs.append(self.gui.appdata.nzbs[i])
            self.gui.appdata.nzbs = newnzbs[:]
            if self.gui.appdata.nzbs[0] != old_first_nzb:
                self.gui.update_first_appdata_nzb()
            self.gui.update_liststore()
            self.gui.update_liststore_dldata()
            self.gui.set_buttons_insensitive()
            self.gui.guiqueue.put(("order_changed", None))
        print("full down clicked")

    def on_button_up_clicked(self, button):
        do_update_dldata = False
        with self.gui.lock:
            old_first_nzb = self.gui.appdata.nzbs[0]
            ros = [(i, self.gui.appdata.nzbs[i]) for i, ro in enumerate(self.gui.nzblist_liststore) if ro[6]]
            for i, r in ros:
                if i == 1:
                    do_update_dldata = True
                if i == 0:
                    break
                oldval = self.gui.appdata.nzbs[i - 1]
                self.gui.appdata.nzbs[i - 1] = r
                self.gui.appdata.nzbs[i] = oldval
            if self.gui.appdata.nzbs[0] != old_first_nzb:
                self.gui.update_first_appdata_nzb()
            self.gui.update_liststore()
            if do_update_dldata:
                self.gui.update_liststore_dldata()
            self.gui.set_buttons_insensitive()
            self.gui.guiqueue.put(("order_changed", None))
        print("up clicked")

    def on_button_down_clicked(self, button):
        do_update_dldata = False
        with self.gui.lock:
            old_first_nzb = self.gui.appdata.nzbs[0]
            ros = [(i, self.gui.appdata.nzbs[i]) for i, ro in enumerate(self.gui.nzblist_liststore) if ro[6]]
            for i, r in reversed(ros):
                if i == 0:
                    do_update_dldata = True
                if i == len(self.gui.appdata.nzbs) - 1:
                    break
                oldval = self.gui.appdata.nzbs[i + 1]
                self.gui.appdata.nzbs[i + 1] = r
                self.gui.appdata.nzbs[i] = oldval
            if self.gui.appdata.nzbs[0] != old_first_nzb:
                self.gui.update_first_appdata_nzb()
            self.gui.update_liststore()
            if do_update_dldata:
                self.gui.update_liststore_dldata()
            self.gui.set_buttons_insensitive()
            self.gui.guiqueue.put(("order_changed", None))
        print("down clicked")

    def on_button_nzbadd_clicked(self, button):
        dialog = Gtk.FileChooserDialog("Choose NZB file(s)", self.gui.window, Gtk.FileChooserAction.OPEN,
                                       (Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL,
                                        Gtk.STOCK_OPEN, Gtk.ResponseType.OK))
        Gtk.FileChooser.set_select_multiple(dialog, True)
        self.add_nzb_filters(dialog)
        nzb_selected = None
        response = dialog.run()
        if response == Gtk.ResponseType.OK:
            button.set_sensitive(False)
            nzb_selected = dialog.get_filenames()
            self.gui.guiqueue.put(("nzb_added", (nzb_selected, button)))
        elif response == Gtk.ResponseType.CANCEL:
            pass
        dialog.destroy()

    def add_nzb_filters(self, dialog):
        filter_text = Gtk.FileFilter()
        filter_text.set_name("NZB files")
        filter_text.add_mime_type("application/x-nzb")
        dialog.add_filter(filter_text)

        filter_any = Gtk.FileFilter()
        filter_any.set_name("Any files")
        filter_any.add_pattern("*")
        dialog.add_filter(filter_any)

    def on_button_interrupt_clicked(self, button):
        dialog = ConfirmDialog(self.gui.window, "Do you really want to stop/move these NZBs ?")
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.CANCEL:
            return
        # here the same as delete
        with self.gui.lock:
            self.gui.remove_selected_from_list()
            self.gui.update_liststore()
            self.gui.update_liststore_dldata()
            self.gui.set_buttons_insensitive()
            self.gui.guiqueue.put(("stopped_moved", None))
        # msg an main: params = newnbzlist, moved_nzbs
        # dann dort: wie "SET_NZB_ORDER", nur ohne delete sondern status change

        print("interrupt clicked")

    def on_button_delete_clicked(self, button):
        # todo: appdata.nzbs -> update_liststore
        dialog = ConfirmDialog(self.gui.window, "Do you really want to delete these NZBs ?")
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.CANCEL:
            return
        with self.gui.lock:
            self.gui.remove_selected_from_list()
            self.gui.update_liststore()
            self.gui.update_liststore_dldata()
            self.gui.set_buttons_insensitive()
            self.gui.guiqueue.put(("order_changed", None))
        print("delete clicked")

    def on_win_destroy(self, *args):
        print("quit!")


# ******************************************************************************************************
# *** Addtl stuff

class ConfirmDialog(Gtk.Dialog):
    def __init__(self, parent, txt):
        Gtk.Dialog.__init__(self, "My Dialog", parent, 9, (Gtk.STOCK_OK, Gtk.ResponseType.OK,
                                                           Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL))
        self.set_default_size(150, 100)
        self.set_border_width(10)
        self.set_modal(True)
        # self.set_property("button-spacing", 10)
        label = Gtk.Label(txt)
        box = self.get_content_area()
        box.add(label)
        self.show_all()


class AppData:
    def __init__(self, lock):
        self.lock = lock
        self.mbitsec = 0
        self.nzbs = []
        self.overall_size = 0
        self.gbdown = 0
        self.servers = [("EWEKA", 40), ("BUCKETNEWS", 15), ("TWEAK", 0)]
        self.dl_running = True
        self.max_mbitsec = 0
        # crit_art_health is taken from server
        self.crit_art_health = 0.95
        self.crit_conn_health = 0.5
        self.sortednzblist = None
        self.sortednzbhistorylist = None
        self.dldata = None
        self.logdata = None
        self.article_health = 0
        self.connection_health = 0
        self.fulldata = None
        self.closeall = False
        self.nzbs_history = []
        self.overall_eta = 0
