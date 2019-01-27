import gi
import threading
import queue
import datetime
import zmq
import time
import configparser
import sys
from threading import Thread
from .mplogging import setup_logger, whoami
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, Gio, Gdk, GdkPixbuf, GLib, Pango


__appname__ = "Ginzibix"
__version__ = "0.01 pre-alpha"
__author__ = "dermatty"

GBXICON = "lib/gzbx1.png"
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


def get_cut_nzbname(nzbname):
    if len(nzbname) <= MAX_NZB_LEN:
        return nzbname
    else:
        return nzbname[:MAX_NZB_LEN - 8] + "[..]" + nzbname[-4:]


def get_cut_msg(msg):
    if len(msg) <= MAX_MSG_LEN:
        return msg
    else:
        return msg[:MAX_MSG_LEN - 8] + "[..]" + msg[-4:]


def get_bg_color(n_status_s):
    bgcol = "white"
    if n_status_s == "preprocessing":
        bgcol = "beige"
    elif n_status_s == "queued":
        bgcol = "khaki"
    elif n_status_s == "downloading":
        bgcol = "yellow"
    elif n_status_s == "postprocessing":
        bgcol = "yellow green"
    elif n_status_s == "success":
        bgcol = "lime green"
    elif n_status_s == "failed" or n_status_s == "unknown":
        bgcol = "red"
    else:
        bgcol = "white"
    return bgcol


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
        self.max_mbitsec = 100
        self.autocal_mmbit = False
        # crit_art_health is taken from server
        self.crit_art_health = 0.95
        self.crit_conn_health = 0.5
        self.sortednzblist = None
        self.sortednzbhistorylist = None
        self.dldata = None
        self.netstat_mbitcur = None
        self.logdata = None
        self.article_health = 0
        self.connection_health = 0
        self.fulldata = None
        self.closeall = False
        self.nzbs_history = []


class AppWindow(Gtk.ApplicationWindow):

    def __init__(self, app, dirs, cfg_file, logger):
        # data
        self.logger = logger
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
            Gtk.main_quit()
        self.appdata = AppData(self.lock)
        self.read_config_file()
        self.dl_running = True
        self.nzb_status_string = ""

        self.win = Gtk.Window.__init__(self, title=__appname__, application=app)

        self.connect("destroy", self.closeall)

        try:
            self.set_icon_from_file(GBXICON)
        except GLib.GError as e:
            self.logger.error(whoami() + str(e) + "cannot find icon file!" + GBXICON)

        self.guiqueue = queue.Queue()

        self.lock = threading.Lock()
        self.guipoller = GUI_Poller(self.lock, self.appdata, self.update_mainwindow, self.toggle_buttons, self.guiqueue, self.logger,
                                        delay=self.update_delay, host=self.host, port=self.port)
        self.guipoller.start()

        # init main window
        self.set_border_width(10)
        self.set_wmclass(__appname__, __appname__)
        self.header_bar()

        box_primary = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        self.add(box_primary)

        box_main = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        box_primary.pack_end(box_main, True, True, 0)

        # stack
        stack = Gtk.Stack()
        stack.set_transition_type(Gtk.StackTransitionType.SLIDE_LEFT_RIGHT)
        stack.set_transition_duration(10)

        stack_switcher = Gtk.StackSidebar()
        stack_switcher.set_stack(stack)
        box_primary.pack_end(stack_switcher, False, False, 0)

        self.stacknzb_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        stack.add_titled(self.stacknzb_box, "nzbqueue", "NZB QUEUE")
        self.show_nzb_stack(self.stacknzb_box)
        self.stackdetails_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
        stack.add_titled(self.stackdetails_box, "nzbhistory", "NZB HISTORY")
        self.show_details_stack(self.stackdetails_box)
        self.stacklogs_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
        stack.add_titled(self.stacklogs_box, "settings", "SETTINGS")
        self.stacksearch_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
        stack.add_titled(self.stacksearch_box, "search", "SEARCH")

        # levelbars; Mbit, article_health, connection_health
        frame1 = Gtk.Frame()
        frame1.set_label("Speed / Health")
        box_main.pack_start(frame1, False, False, 10)

        boxvertical = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        frame1.add(boxvertical)

        self.box_levelbar = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL)
        self.box_levelbar.set_property("margin-left", 20)
        self.box_levelbar.set_property("margin-right", 20)

        # levelbar "Speed mbit"
        speedlabel = Gtk.Label("Speed ")
        self.box_levelbar.pack_start(speedlabel, False, False, 0)
        self.levelbar = Gtk.LevelBar.new_for_interval(0, 1)
        self.levelbar.set_mode(Gtk.LevelBarMode.CONTINUOUS)
        self.levelbar.set_value(0)
        self.levelbar.set_tooltip_text("Max = " + str(self.appdata.max_mbitsec))
        self.box_levelbar.pack_start(self.levelbar, True, True, 0)
        self.mbitlabel2 = Gtk.Label(None)
        if self.appdata.mbitsec > 0:
            mbitstr = str(int(self.appdata.mbitsec)) + " MBit/s"
            self.mbitlabel2.set_text(mbitstr.rjust(11))
        else:
            self.mbitlabel2.set_text("")
        self.box_levelbar.pack_start(self.mbitlabel2, False, False, 0)
        boxvertical.pack_start(self.box_levelbar, True, True, 0)

        # levelbars "article health"
        self.box_health = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL)
        self.box_health.set_property("margin-left", 20)
        self.box_health.set_property("margin-right", 20)
        self.box_health.pack_start(Gtk.Label("Article health "), False, False, 0)
        self.levelbar_arthealth = Gtk.LevelBar.new_for_interval(0, 1)
        self.box_health.pack_start(self.levelbar_arthealth, True, True, 0)
        self.arthealth_label = Gtk.Label(None)
        self.box_health.pack_start(self.arthealth_label, False, False, 0)
        self.levelbar_arthealth.set_mode(Gtk.LevelBarMode.CONTINUOUS)

        self.artconn_label = Gtk.Label(None)
        self.box_health.pack_end(self.artconn_label, False, False, 0)
        self.levelbar_connhealth = Gtk.LevelBar.new_for_interval(0, 1)
        self.box_health.pack_end(self.levelbar_connhealth, True, True, 0)
        self.levelbar_connhealth.set_mode(Gtk.LevelBarMode.CONTINUOUS)
        self.levelbar_connhealth.set_value(0)
        self.box_health.pack_end(Gtk.Label("   Conn. health "), False, False, 0)
        self.levelbar_arthealth.set_mode(Gtk.LevelBarMode.CONTINUOUS)
        self.update_crit_health_levelbars()
        self.update_health()

        boxvertical.pack_start(self.box_health, True, True, 0)

        box_main.pack_start(stack, True, True, 0)

    def show_details_stack(self, stackdetails_box):
        detailsframe2 = Gtk.Frame()
        detailsframe2.set_label("NZB History")
        stackdetails_box.pack_start(detailsframe2, True, True, 0)
        # scrolled window
        detailsscrolled_window = Gtk.ScrolledWindow()
        detailsscrolled_window.set_border_width(10)
        detailsscrolled_window.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        detailsscrolled_window.set_property("min-content-height", 380)
        detailsframe2.add(detailsscrolled_window)
        #detailslistbox = Gtk.ListBox()
        #detailsrow = Gtk.ListBoxRow()
        # populate liststore_nzbhistory
        self.liststore_nzbhistory = Gtk.ListStore(str, int)
        self.update_nzbhistory_liststore()
        self.treeview_history = Gtk.TreeView(model=self.liststore_nzbhistory)
        self.treeview_history.set_reorderable(False)
        sel = self.treeview_history.get_selection()
        sel.set_mode(Gtk.SelectionMode.NONE)
        # treeview.get_selection().connect("changed", self.on_selection_changed)
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
        # final
        #detailsrow.add(self.treeview_history)
        #detailslistbox.add(detailsrow)
        detailsscrolled_window.add(self.treeview_history)

    def show_nzb_stack(self, stacknzb_box):
        frame2 = Gtk.Frame()
        frame2.set_label("NZB queue")
        stacknzb_box.pack_start(frame2, True, True, 0)
        # scrolled window
        scrolled_window = Gtk.ScrolledWindow()
        scrolled_window.set_border_width(10)
        scrolled_window.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        scrolled_window.set_property("min-content-height", 380)
        frame2.add(scrolled_window)
        # listbox
        #listbox = Gtk.ListBox()
        #row = Gtk.ListBoxRow()
        # populate liststore
        self.liststore = Gtk.ListStore(str, int, float, float, str, str, bool, str, str)
        self.update_liststore()
        # set treeview + actions
        self.treeview_nzb = Gtk.TreeView(model=self.liststore)
        self.treeview_nzb.set_reorderable(False)
        sel = self.treeview_nzb.get_selection()
        sel.set_mode(Gtk.SelectionMode.NONE)
        # self.treeview_nzb.get_selection().connect("changed", self.on_selection_changed)
        # 0th selection toggled
        renderer_toggle = Gtk.CellRendererToggle()
        renderer_toggle.connect("toggled", self.on_inverted_toggled)
        column_toggle = Gtk.TreeViewColumn("Select", renderer_toggle, active=6)
        self.treeview_nzb.append_column(column_toggle)
        # 1st column: NZB name
        renderer_text0 = Gtk.CellRendererText()
        column_text0 = Gtk.TreeViewColumn("NZB name", renderer_text0, text=0)
        column_text0.set_expand(True)
        column_text0.set_min_width(320)
        self.treeview_nzb.append_column(column_text0)
        # 2nd: progressbar
        renderer_progress = Gtk.CellRendererProgress()
        column_progress = Gtk.TreeViewColumn("Progress", renderer_progress, value=1, text=5)
        column_progress.set_min_width(260)
        column_progress.set_expand(True)
        self.treeview_nzb.append_column(column_progress)
        # 3rd downloaded GiN
        renderer_text1 = Gtk.CellRendererText()
        column_text1 = Gtk.TreeViewColumn("Downloaded", renderer_text1, text=2)
        column_text1.set_cell_data_func(renderer_text1, lambda col, cell, model, iter, unused:
                                        cell.set_property("text", "{0:.2f}".format(model.get(iter, 2)[0]) + " GiB"))
        self.treeview_nzb.append_column(column_text1)
        # 4th overall GiB
        renderer_text2 = Gtk.CellRendererText()
        column_text2 = Gtk.TreeViewColumn("Overall", renderer_text2, text=3)
        column_text2.set_cell_data_func(renderer_text2, lambda col, cell, model, iter, unused:
                                        cell.set_property("text", "{0:.2f}".format(model.get(iter, 3)[0]) + " GiB"))
        column_text2.set_min_width(80)
        self.treeview_nzb.append_column(column_text2)
        # 5th Eta
        renderer_text3 = Gtk.CellRendererText()
        column_text3 = Gtk.TreeViewColumn("Eta", renderer_text3, text=4)
        column_text3.set_min_width(80)
        self.treeview_nzb.append_column(column_text3)
        # 7th status
        renderer_text7 = Gtk.CellRendererText()
        column_text7 = Gtk.TreeViewColumn("Status", renderer_text7, text=7, background=8)
        column_text7.set_min_width(80)
        self.treeview_nzb.append_column(column_text7)
        # final
        #row.add(self.treeview_nzb)
        #listbox.add(row)
        scrolled_window.add(self.treeview_nzb)

        self.gridbuttonlist = self.add_action_bar(stacknzb_box)

        # treeview for logs
        # msg, level, tt, bg, fg
        frame3 = Gtk.Frame()
        frame3.set_label("Logs")
        stacknzb_box.pack_start(frame3, True, True, 0)
        scrolled_window_log = Gtk.ScrolledWindow()
        scrolled_window_log.set_border_width(10)
        scrolled_window_log.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        scrolled_window_log.set_property("min-content-height", 140)
        frame3.add(scrolled_window_log)

        #loglistbox = Gtk.ListBox()
        #logrow = Gtk.ListBoxRow()

        self.logliststore = Gtk.ListStore(str, str, str, str, str)
        logtreeview = Gtk.TreeView(model=self.logliststore)

        renderer_log4 = Gtk.CellRendererText()
        column_log4 = Gtk.TreeViewColumn("Time", renderer_log4, text=2, background=3, foreground=4)
        column_log4.set_min_width(80)
        logtreeview.append_column(column_log4)

        renderer_log3 = Gtk.CellRendererText()
        column_log3 = Gtk.TreeViewColumn("Level", renderer_log3, text=1, background=3, foreground=4)
        column_log3.set_min_width(80)
        logtreeview.append_column(column_log3)

        renderer_log2 = Gtk.CellRendererText()
        column_log2 = Gtk.TreeViewColumn("Message", renderer_log2, text=0, background=3, foreground=4)
        column_log2.set_expand(True)
        column_log2.set_min_width(520)
        logtreeview.append_column(column_log2)

        #logrow.add(logtreeview)
        #loglistbox.add(logrow)
        scrolled_window_log.add(logtreeview)

    def add_action_bar(self, container):
        # box for record/stop/.. selected
        box_media = Gtk.ActionBar()
        box_media_expand = False
        box_media_fill = False
        box_media_padd = 1
        container.pack_start(box_media, box_media_expand, box_media_fill, box_media_padd)
        gridbuttonlist = []
        # button full up
        button_full_up = Gtk.Button.new_from_icon_name("arrow-up-double", Gtk.IconSize.SMALL_TOOLBAR)
        button_full_up.set_sensitive(False)
        button_full_up.connect("clicked", self.on_buttonfullup_clicked)
        box_media.pack_start(button_full_up)
        button_full_up.set_tooltip_text("Move NZB(s) to top")
        gridbuttonlist.append(button_full_up)
        # button up
        button_up = Gtk.Button.new_from_icon_name("arrow-up", Gtk.IconSize.SMALL_TOOLBAR)
        button_up.set_sensitive(False)
        button_up.connect("clicked", self.on_buttonup_clicked)
        box_media.pack_start(button_up)
        button_up.set_tooltip_text("Move NZB(s) 1 up")
        gridbuttonlist.append(button_up)
        # button down
        button_down = Gtk.Button.new_from_icon_name("arrow-down", Gtk.IconSize.SMALL_TOOLBAR)
        button_down.set_sensitive(False)
        button_down.connect("clicked", self.on_buttondown_clicked)
        box_media.pack_start(button_down)
        button_down.set_tooltip_text("Move NZB(s) 1 down")
        gridbuttonlist.append(button_down)
        # button full down
        button_full_down = Gtk.Button.new_from_icon_name("arrow-down-double", Gtk.IconSize.SMALL_TOOLBAR)
        button_full_down.set_sensitive(False)
        button_full_down.connect("clicked", self.on_buttonfulldown_clicked)
        box_media.pack_start(button_full_down)
        button_full_down.set_tooltip_text("Move NZB(s) to bottom")
        gridbuttonlist.append(button_full_down)
        # delete
        button_delete = Gtk.Button.new_from_icon_name("gtk-delete", Gtk.IconSize.SMALL_TOOLBAR)
        button_delete.set_sensitive(False)
        button_delete.connect("clicked", self.on_buttondelete_clicked)
        box_media.pack_end(button_delete)
        button_delete.set_tooltip_text("Delete NZB(s)")
        gridbuttonlist.append(button_delete)
        # stop downloading / move to history
        button_stopmove = Gtk.Button.new_from_icon_name("media-playback-stop", Gtk.IconSize.SMALL_TOOLBAR)
        button_stopmove.set_sensitive(False)
        button_stopmove.connect("clicked", self.on_buttonstopmove_clicked)
        button_stopmove.set_tooltip_text("Stop downloading / move to history")
        box_media.pack_end(button_stopmove)
        gridbuttonlist.append(button_stopmove)
        # add
        button_add = Gtk.Button.new_from_icon_name("list-add", Gtk.IconSize.SMALL_TOOLBAR)
        button_add.set_sensitive(True)
        button_add.connect("clicked", self.on_buttonadd_clicked)
        button_add.set_tooltip_text("Add NZB from file")
        box_media.pack_end(button_add)
        # center, restart z.b
        # action_bar.set_center_widget (secondary_box)
        return gridbuttonlist

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
        # max. mbit/seconds
        try:
            self.appdata.max_mbitsec = int(self.cfg["GTKGUI"]["MAX_MBITSEC"])
            assert(self.appdata.max_mbitsec > 1 and self.appdata.max_mbitsec <= 10000)
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ", setting max_mbitsec to default 100")
            self.appdata.max_mbitsec = 100
        # autocal_mmbit
        try:
            self.appdata.autocal_mmbit = True if self.cfg["GTKGUI"]["AUTOCAL_MMBIT"].lower() == "yes" else False
        except Exception as e:
            self.logger.warning(whoami() + str(e) + ", setting autocal_mmbit to default False")
            self.appdata.autocal_mmbit = False

    # stop/move button
    def on_buttonstopmove_clicked(self, button):
        dialog = ConfirmDialog(self, "Do you really want to stop/move these NZBs ?")
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.CANCEL:
            return
        # here the same as delete
        with self.lock:
            self.remove_selected_from_list()
            self.update_liststore()
            self.update_liststore_dldata()
            self.set_buttons_insensitive()
            self.guiqueue.put(("stopped_moved", None))
        # msg an main: params = newnbzlist, moved_nzbs
        # dann dort: wie "SET_NZB_ORDER", nur ohne delete sondern status change

    # add nzb button
    def on_buttonadd_clicked(self, button):
        dialog = Gtk.FileChooserDialog("Choose NZB file(s)", self, Gtk.FileChooserAction.OPEN,
                                       (Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL,
                                        Gtk.STOCK_OPEN, Gtk.ResponseType.OK))
        Gtk.FileChooser.set_select_multiple(dialog, True)
        self.add_nzb_filters(dialog)
        nzb_selected = None
        response = dialog.run()
        if response == Gtk.ResponseType.OK:
            button.set_sensitive(False)
            nzb_selected = dialog.get_filenames()
            self.guiqueue.put(("nzb_added", (nzb_selected, button)))
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

    def remove_selected_from_list(self):
        old_first_nzb = self.appdata.nzbs[0]
        newnzbs = []
        for i, ro in enumerate(self.liststore):
            if not ro[6]:
                newnzbs.append(self.appdata.nzbs[i])
        self.appdata.nzbs = newnzbs[:]
        if self.appdata.nzbs:
            if self.appdata.nzbs[0] != old_first_nzb:
                self.update_first_appdata_nzb()

    def on_buttondelete_clicked(self, button):
        # todo: appdata.nzbs -> update_liststore
        dialog = ConfirmDialog(self, "Do you really want to delete these NZBs ?")
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.CANCEL:
            return
        with self.lock:
            self.remove_selected_from_list()
            self.update_liststore()
            self.update_liststore_dldata()
            self.set_buttons_insensitive()
            self.guiqueue.put(("order_changed", None))

    def on_buttonup_clicked(self, button):
        do_update_dldata = False
        with self.lock:
            old_first_nzb = self.appdata.nzbs[0]
            ros = [(i, self.appdata.nzbs[i]) for i, ro in enumerate(self.liststore) if ro[6]]
            for i, r in ros:
                if i == 1:
                    do_update_dldata = True
                if i == 0:
                    break
                oldval = self.appdata.nzbs[i - 1]
                self.appdata.nzbs[i - 1] = r
                self.appdata.nzbs[i] = oldval
            if self.appdata.nzbs[0] != old_first_nzb:
                self.update_first_appdata_nzb()
            self.update_liststore()
            if do_update_dldata:
                self.update_liststore_dldata()
            self.set_buttons_insensitive()
            self.guiqueue.put(("order_changed", None))

    def on_buttondown_clicked(self, button):
        do_update_dldata = False
        with self.lock:
            old_first_nzb = self.appdata.nzbs[0]
            ros = [(i, self.appdata.nzbs[i]) for i, ro in enumerate(self.liststore) if ro[6]]
            for i, r in reversed(ros):
                if i == 0:
                    do_update_dldata = True
                if i == len(self.appdata.nzbs) - 1:
                    break
                oldval = self.appdata.nzbs[i + 1]
                self.appdata.nzbs[i + 1] = r
                self.appdata.nzbs[i] = oldval
            if self.appdata.nzbs[0] != old_first_nzb:
                self.update_first_appdata_nzb()
            self.update_liststore()
            if do_update_dldata:
                self.update_liststore_dldata()
            self.set_buttons_insensitive()
            self.guiqueue.put(("order_changed", None))

    def on_buttonfullup_clicked(self, button):
        with self.lock:
            old_first_nzb = self.appdata.nzbs[0]
            newnzbs = []
            for i, ro in enumerate(self.liststore):
                if ro[6]:
                    newnzbs.append(self.appdata.nzbs[i])
            for i, ro in enumerate(self.liststore):
                if not ro[6]:
                    newnzbs.append(self.appdata.nzbs[i])
            self.appdata.nzbs = newnzbs[:]
            if self.appdata.nzbs[0] != old_first_nzb:
                self.update_first_appdata_nzb()
            self.update_liststore()
            self.update_liststore_dldata()
            self.set_buttons_insensitive()
            self.guiqueue.put(("order_changed", None))

    def on_buttonfulldown_clicked(self, button):
        with self.lock:
            old_first_nzb = self.appdata.nzbs[0]
            newnzbs = []
            for i, ro in enumerate(self.liststore):
                if not ro[6]:
                    newnzbs.append(self.appdata.nzbs[i])
            for i, ro in enumerate(self.liststore):
                if ro[6]:
                    newnzbs.append(self.appdata.nzbs[i])
            self.appdata.nzbs = newnzbs[:]
            if self.appdata.nzbs[0] != old_first_nzb:
                self.update_first_appdata_nzb()
            self.update_liststore()
            self.update_liststore_dldata()
            self.set_buttons_insensitive()
            self.guiqueue.put(("order_changed", None))

    def on_inverted_toggled(self, widget, path):
        with self.lock:
            self.liststore[path][6] = not self.liststore[path][6]
            i = int(path)
            newnzb = list(self.appdata.nzbs[i])
            newnzb[6] = self.liststore[path][6]
            self.appdata.nzbs[i] = tuple(newnzb)
            self.toggle_buttons()

    def update_health(self):
        self.levelbar_connhealth.set_value(self.appdata.connection_health)
        self.levelbar_arthealth.set_value(self.appdata.article_health)
        if self.appdata.article_health > 0:
            arth_str = str(int(self.appdata.article_health * 100)) + "%"
            self.arthealth_label.set_text(arth_str.rjust(5))
        else:
            self.arthealth_label.set_text(" " * 5)
        if self.appdata.connection_health > 0:
            conn_str = str(int(self.appdata.connection_health * 100)) + "%"
            self.artconn_label.set_text(conn_str.rjust(5))
        else:
            self.artconn_label.set_text(" " * 5)

    def update_logstore(self):
        # only show msgs for current nzb
        self.logliststore.clear()
        if not self.appdata.nzbname:
            return
        try:
            loglist = self.appdata.fulldata[self.appdata.nzbname]["msg"][:]
        except Exception as e:
            return
        for msg0, ts0, level0 in loglist:
            log_as_list = []
            # msg, level, tt, bg, fg
            # log_as_list.append(get_cut_nzbname(self.appdata.nzbname))
            log_as_list.append(get_cut_msg(msg0))
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
            self.logliststore.append(log_as_list)

    def update_logstore2(self):
        self.logliststore.clear()
        for i, log in enumerate(self.appdata.logdata):
            log_as_list = list(log)
            if log_as_list[3] == 0:
                log_as_list[3] = ""
            else:
                log_as_list[3] = str(datetime.datetime.fromtimestamp(log_as_list[3]).strftime('%Y-%m-%d %H:%M:%S'))
            log_as_list[0] = get_cut_nzbname(log_as_list[0])
            log_as_list[1] = get_cut_msg(log_as_list[1])
            fg = "black"
            if log_as_list[2] == "info":
                bg = "royal Blue"
                fg = "white"
            elif log_as_list[2] == "warning":
                bg = "orange"
                fg = "white"
            elif log_as_list[2] == "error":
                bg = "red"
                fg = "white"
            elif log_as_list[2] == "success":
                bg = "green"
                fg = "white"
            else:
                bg = "white"
            log_as_list.append(bg)
            log_as_list.append(fg)
            self.logliststore.append(log_as_list)

    def update_nzbhistory_liststore(self):
        self.liststore_nzbhistory.clear()
        for i, nzb in enumerate(self.appdata.nzbs_history):
            nzb_as_list = list(nzb)
            self.liststore_nzbhistory.append(nzb_as_list)

    def update_liststore(self, only_eta=False):
        # n_name, n_perc, n_dl, n_size, etastr, str(n_perc) + "%", selected, n_status))
        if only_eta:
            for i, nzb in enumerate(self.appdata.nzbs):
                # skip first one as it will be updated anyway
                if i == 0:
                    continue
                try:
                    path = Gtk.TreePath(i)
                    iter = self.liststore.get_iter(path)
                except Exception as e:
                    self.logger.debug(whoami() + str(e))
                    continue
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
                self.liststore.set_value(iter, 4, etastr)
            return

        self.liststore.clear()
        for i, nzb in enumerate(self.appdata.nzbs):
            nzb_as_list = list(nzb)
            nzb_as_list[0] = get_cut_nzbname(nzb_as_list[0])
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
            self.liststore.append(nzb_as_list)

    def update_liststore_dldata(self):
        if len(self.liststore) == 0:
            self.levelbar.set_value(0)
            self.mbitlabel2.set_text("")
            self.levelbar_connhealth.set_value(0)
            self.levelbar_arthealth.set_value(0)
            return
        path = Gtk.TreePath(0)
        iter = self.liststore.get_iter(path)

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

        self.liststore.set_value(iter, 1, n_perc)
        self.liststore.set_value(iter, 2, n_dl)
        self.liststore.set_value(iter, 3, n_size)
        self.liststore.set_value(iter, 5, str(n_perc) + "%")
        self.liststore.set_value(iter, 7, self.nzb_status_string)
        self.liststore.set_value(iter, 8, n_bgcolor)
        try:
            if self.appdata.mbitsec > 0 and self.dl_running:
                eta0 = (((self.appdata.overall_size - self.appdata.gbdown) * 1024) / (self.appdata.mbitsec / 8))
                if eta0 < 0:
                    eta0 = 0
                etastr = str(datetime.timedelta(seconds=int(eta0)))
            else:
                etastr = "-"
        except Exception:
            etastr = "-"
        self.liststore.set_value(iter, 4, etastr)
        if len(self.appdata.nzbs) > 0:
            newnzb = (self.appdata.nzbs[0][0], n_perc, n_dl, n_size, etastr, str(n_perc) + "%", self.appdata.nzbs[0][6], self.appdata.nzbs[0][7])
            self.appdata.nzbs[0] = newnzb
            if self.appdata.mbitsec > 0 and self.appdata.dl_running:
                self.levelbar.set_value(self.appdata.mbitsec / self.appdata.max_mbitsec)
                mbitsecstr = str(int(self.appdata.mbitsec)) + " MBit/s"
                self.mbitlabel2.set_text(mbitsecstr.rjust(11))
            else:
                self.levelbar.set_value(0)
                self.mbitlabel2.set_text("")
        else:
            self.levelbar.set_value(0)
            self.mbitlabel2.set_text("")

    def toggle_buttons(self):
        one_is_selected = False
        if not one_is_selected:
            for ls in range(len(self.liststore)):
                path0 = Gtk.TreePath(ls)
                if self.liststore[path0][6]:
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

    def on_selection_changed(self, selection):
        self.show_nzb_stack(self.stacknzb_box)
        pass

    def on_selection_changed_nzbhistory(self, selection):
        # (model, iter) = selection.get_selected()
        pass

    def header_bar(self):
        hb = Gtk.HeaderBar(spacing=20)
        hb.set_show_close_button(True)
        hb.props.title = __appname__
        self.set_titlebar(hb)

        button_startstop = Gtk.Button()
        button_startstop.set_property("margin-left", 2)
        icon = Gio.ThemedIcon(name="media-playback-pause")
        image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
        button_startstop.add(image)
        button_startstop.connect("clicked", self.on_buttonstartstop_clicked)
        button_startstop.set_tooltip_text("Pause download")
        hb.pack_start(button_startstop)

        button_settings = Gtk.Button()
        icon2 = Gio.ThemedIcon(name="open-menu")
        image2 = Gtk.Image.new_from_gicon(icon2, Gtk.IconSize.BUTTON)
        button_settings.add(image2)
        # button_settings.connect("clicked", self.on_buttonsettings_clicked)
        button_settings.set_tooltip_text("Settings")
        hb.pack_end(button_settings)

    def on_buttonstartstop_clicked(self, button):
        with self.lock:
            self.appdata.dl_running = not self.appdata.dl_running
        self.guiqueue.put(("dl_running", self.appdata.dl_running))
        if self.appdata.dl_running:
            icon = Gio.ThemedIcon(name="media-playback-pause")
            image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
            button.set_image(image)
        else:
            icon = Gio.ThemedIcon(name="media-playback-start")
            image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
            button.set_image(image)
        self.update_liststore_dldata()
        self.update_liststore()

    def closeall(self, a):
        # Gtk.main_quit()
        if self.appdata.autocal_mmbit:
            self.logger.debug(whoami() + "updating configfile")
            self.cfg["GTKGUI"]["MAX_MBITSEC"] = str(int(self.appdata.max_mbitsec))
            with open(self.cfg_file, 'w') as configfile:
                self.cfg.write(configfile)
        self.appdata.closeall = True
        self.guiqueue.put(("closeall", None))
        while self.appdata.closeall:
            time.sleep(0.1)
        self.guipoller.stop()

    def update_crit_health_levelbars(self):
        crit_art_health = self.appdata.crit_art_health
        self.levelbar_arthealth.set_tooltip_text("Critical = " + str(int(float("{0:.4f}".format(crit_art_health)) * 100)) + "%")
        self.levelbar_arthealth.add_offset_value(Gtk.LEVEL_BAR_OFFSET_LOW, crit_art_health + (1 - crit_art_health) * 0.25)
        self.levelbar_arthealth.add_offset_value(Gtk.LEVEL_BAR_OFFSET_HIGH, crit_art_health + (1 - crit_art_health) * 0.75)
        crit_conn_health = self.appdata.crit_conn_health
        self.levelbar_connhealth.add_offset_value(Gtk.LEVEL_BAR_OFFSET_LOW, crit_conn_health + (1 - crit_conn_health) * 0.25)
        self.levelbar_connhealth.add_offset_value(Gtk.LEVEL_BAR_OFFSET_HIGH, crit_conn_health + (1 - crit_conn_health) * 0.75)
        self.levelbar_connhealth.set_tooltip_text("Critical = " + str(int(float("{0:.4f}".format(crit_conn_health)) * 100)) + "%")

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

        # health
        if dlconfig:
            crit_art_health, crit_conn_health = dlconfig
            newhealth = False
            if crit_art_health != self.appdata.crit_art_health:
                self.appdata.crit_art_health = crit_art_health
                newhealth = True
            if crit_conn_health != self.appdata.crit_conn_health:
                self.appdata.crit_conn_health = crit_conn_health
                newhealth = True
            if newhealth:
                self.update_crit_health_levelbars()

        if (article_health is not None and self.appdata.article_health != article_health):
            self.appdata.article_health = article_health
            self.update_health()

        if (connection_health is not None and self.appdata.connection_health != connection_health):
            self.appdata.connection_health = connection_health
            self.update_health()

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
                    if self.appdata.autocal_mmbit and mbitseccurr > self.appdata.max_mbitsec:
                        self.appdata.max_mbitsec = mbitseccurr
                        self.levelbar.set_tooltip_text("Max = " + str(self.appdata.max_mbitsec))
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
                    self.appdata.netstat_mbitcur = netstat_mbitcur
        return False

    def update_first_appdata_nzb(self):
        if self.appdata.nzbs:
            _, _, n_dl, n_size, _, _, _, _ = self.appdata.nzbs[0]
            self.appdata.gbdown = n_dl
            self.appdata.overall_size = n_size


class Application(Gtk.Application):

    def __init__(self, dirs, cfg_file, mp_loggerqueue):
        Gtk.Application.__init__(self)
        self.window = None
        self.logger = setup_logger(mp_loggerqueue, __file__)
        self.dirs = dirs
        self.cfg_file = cfg_file

    def do_activate(self):
        self.window = AppWindow(self, self.dirs, self.cfg_file, self.logger)
        self.window.show_all()

    def do_startup(self):
        Gtk.Application.do_startup(self)

        action = Gio.SimpleAction.new("settings", None)
        action.connect("activate", self.on_settings)
        self.add_action(action)

        action = Gio.SimpleAction.new("about", None)
        action.connect("activate", self.on_about)
        self.add_action(action)

        action = Gio.SimpleAction.new("quit", None)
        action.connect("activate", self.on_quit)
        self.add_action(action)

        builder = Gtk.Builder.new_from_string(MENU_XML, -1)
        self.set_app_menu(builder.get_object("app-menu"))

    def on_settings(self, action, param):
        pass

    def on_about(self, action, param):
        about_dialog = Gtk.AboutDialog(transient_for=self.window, modal=True)
        about_dialog.set_program_name(__appname__)
        about_dialog.set_version(__version__)
        about_dialog.set_copyright("Copyright \xa9 2018 dermatty")
        about_dialog.set_comments("A binary newsreader for the Gnome desktop")
        about_dialog.set_website("https://github.com/dermatty/GINZIBIX")
        about_dialog.set_website_label('Ginzibix on GitHub')
        try:
            about_dialog.set_logo(GdkPixbuf.Pixbuf.new_from_file_at_size(GBXICON, 64, 64))
        except GLib.GError as e:
            self.logger.debug(whoami() + "cannot find icon file!")

        about_dialog.set_license_type(Gtk.License.GPL_3_0)

        about_dialog.present()

    def on_quit(self, action, param):
        self.quit()
        self.window.closeall(None)


# connects to guiconnector
class GUI_Poller(Thread):

    def __init__(self, lock, appdata, update_mainwindow, toggle_buttons, guiqueue, logger, delay=0.5, host="127.0.0.1", port="36603"):
        Thread.__init__(self)
        self.daemon = True
        self.context = zmq.Context()
        self.host = host
        self.port = port
        self.lock = lock
        self.data = None
        self.delay = float(delay)
        self.appdata = appdata
        self.update_mainwindow = update_mainwindow
        self.socket = self.context.socket(zmq.REQ)
        self.logger = logger
        self.event_stopped = threading.Event()
        self.guiqueue = guiqueue
        self.toggle_buttons = toggle_buttons

    def stop(self):
        self.logger.debug(whoami() + "setting event_stopped")
        self.event_stopped.set()

    def run(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        socketurl = "tcp://" + self.host + ":" + self.port
        self.socket.connect(socketurl)
        dl_running = True
        while not self.event_stopped.wait(self.delay):
            # some button pressed, of which main.py should be informed?
            try:
                queue_elem = self.guiqueue.get_nowait()
                self.guiqueue.task_done()
            except (queue.Empty, EOFError, ValueError):
                queue_elem = None
            except Exception as e:
                self.logger.error(whoami() + str(e))
                queue_elem = None

            if queue_elem:
                elem_type, elem_val = queue_elem
                if elem_type == "order_changed":
                    msg0 = "SET_NZB_ORDER"
                    msg0_val = [nzb[0] for nzb in self.appdata.nzbs]
                elif elem_type == "stopped_moved":
                    msg0 = "STOPPED_MOVED"
                    msg0_val = [nzb[0] for nzb in self.appdata.nzbs]
                elif elem_type == "closeall":
                    msg0 = "SET_CLOSEALL"
                    msg0_val = None
                    self.appdata.closeall = True
                elif elem_type == "nzb_added":
                    msg0 = "NZB_ADDED"
                    msg0_val, add_button = elem_val
                elif elem_type == "dl_running":
                    msg0_val = None
                    dl_running_new = elem_val
                    if dl_running != dl_running_new:
                        dl_running = dl_running_new
                        if dl_running:
                            msg0 = "SET_RESUME"
                        else:
                            msg0 = "SET_PAUSE"
                    else:
                        msg0 = None
                else:
                    msg0 = None
                if msg0:
                    try:
                        self.socket.send_pyobj((msg0, msg0_val))
                        datatype, datarec = self.socket.recv_pyobj()
                    except Exception as e:
                        self.logger.error(whoami() + str(e))
                    if elem_type == "nzb_added":
                        add_button.set_sensitive(True)
                    elif elem_type == "closeall":
                        with self.lock:
                            self.appdata.closeall = False
                    elif elem_type in ["order_changed", "stopped_moved"]:
                        GLib.idle_add(self.toggle_buttons)
                        self.logger.debug(whoami() + "order changed ok!")
                else:
                    self.logger.error(whoami() + "cannot interpret element in guiqueue")
            else:
                try:
                    self.socket.send_pyobj(("REQ", None))
                    datatype, datarec = self.socket.recv_pyobj()
                    if datatype == "NOOK":
                        continue
                    elif datatype == "DL_DATA":
                        data, server_config, threads, dl_running, nzb_status_string, netstat_mbitcurr, sortednzblist, sortednzbhistorylist,  \
                            article_health, connection_health, dlconfig, full_data = datarec
                        try:
                            GLib.idle_add(self.update_mainwindow, data, server_config, threads, dl_running, nzb_status_string,
                                          netstat_mbitcurr, sortednzblist, sortednzbhistorylist, article_health, connection_health, dlconfig, full_data)    
                            continue
                        except Exception as e:
                            self.logger.debug(whoami() + str(e))
                except Exception as e:
                    self.logger.error(whoami() + str(e))

        # close socket, join queue & exit guipoller
        self.logger.debug(whoami() + "closing socket")
        try:
            self.socket.close()
            self.context.term()
        except Exception:
            self.logger.warning(whoami())
        self.logger.debug(whoami() + "joining gui_queue")
        while True:
            try:
                queue_elem = self.guiqueue.get_nowait()
                self.guiqueue.task_done()
            except (queue.Empty, EOFError, ValueError):
                break
            except Exception as e:
                self.logger.error(whoami() + str(e))
                break
        self.guiqueue.join()
        self.logger.info(whoami() + "exiting")
        sys.exit()
