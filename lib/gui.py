import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio, Gdk, GdkPixbuf, GLib, Pango
import inspect
import sys


__appname__ = "ginzibix"
__version__ = "0.01 pre-alpha"
__author__ = "dermatty"

GBXICON = "gzbx1.png"


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


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    return outer_func_name + " / #" + outer_func_linenr + ": "


class Application(Gtk.Application):

    def __init__(self):
        self.app = Gtk.Application.new("org.usenet.ginzibix", Gio.ApplicationFlags(0), )
        self.app.connect("startup", self.on_app_startup)
        self.app.connect("activate", self.on_app_activate)
        self.app.connect("shutdown", self.on_app_shutdown)

    def run(self, argv):
        self.app.run(argv)

    def on_app_activate(self, app):
        self.builder = Gtk.Builder()
        self.builder.add_from_file("ginzibix.glade")
        self.builder.add_from_string(MENU_XML)
        self.builder.connect_signals(Handler())
        app.set_app_menu(self.builder.get_object("app-menu"))

        self.obj = self.builder.get_object
        self.window = self.obj("main_window")
        self.window.set_application(self.app)

        # set icon & app name
        self.window.set_icon_from_file(GBXICON)
        self.window.set_wmclass(__appname__, __appname__)

        # set app position
        screen = self.window.get_screen()
        self.max_width = screen.get_width()
        self.max_height = screen.get_height()
        self.n_monitors = screen.get_n_monitors()
        self.window.set_default_size(int(self.max_width/(2 * self.n_monitors)), self.max_height)
        self.window.set_position(Gtk.WindowPosition.CENTER_ALWAYS)

        self.window.show_all()

        # add non-standard actions
        self.add_simple_action("about", self.on_action_about_activated)
        self.add_simple_action("message", self.on_action_message_activated)
        self.add_simple_action("quit", self.on_action_quit_activated)
        self.add_simple_action("settings", self.on_action_settings_activated)

        # set liststore for nzb downloading list
        self.setup_frame_nzblist()
        self.setup_frame_logs()

    def on_app_startup(self, app):
        print("startup activated!")

    def on_app_shutdown(self, app):
        print("shutdown")

    # Application menu actions

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
            about_dialog.set_logo(GdkPixbuf.Pixbuf.new_from_file_at_size(GBXICON, 64, 64))
        except GLib.GError as e:
            print(whoami() + str(e) + "cannot find icon file!")

        about_dialog.set_license_type(Gtk.License.GPL_3_0)
        about_dialog.present()

    def on_action_message_activated(self, actiong, param):
        pass

    def on_action_quit_activated(self, action, param):
        self.app.quit()

    #  liststores / treeviews

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

    # liststore/treeview for nzblist in stack DOWNLOADING
    def setup_frame_nzblist(self):
        self.nzblist_liststore = Gtk.ListStore(str, int, float, float, str, str, bool, str, str)
        # set treeview + actions
        self.treeview_nzblist = Gtk.TreeView(model=self.nzblist_liststore)
        self.treeview_nzblist.set_reorderable(False)
        sel = self.treeview_nzblist.get_selection()
        sel.set_mode(Gtk.SelectionMode.NONE)
        # 0th selection toggled
        renderer_toggle = Gtk.CellRendererToggle()
        # renderer_toggle.connect("toggled", self.on_inverted_toggled)
        column_toggle = Gtk.TreeViewColumn("Select", renderer_toggle, active=6)
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


class Handler:

    def on_button_pause_resume_clicked(self, button):
        print("pause resume clicked!")

    def on_button_settings_clicked(self, button):
        print("settings clicked")

    def on_button_top_clicked(self, button):
        print("full up clicked")

    def on_button_bottom_clicked(self, button):
        print("full down clicked")

    def on_button_up_clicked(self, button):
        print("up clicked")

    def on_button_down_clicked(self, button):
        print("down clicked")

    def on_button_nzbadd_clicked(self, button):
        print("add nzb clicked")

    def on_button_interrupt_clicked(self, button):
        print("interrupt clicked")

    def on_button_delete_clicked(self, button):
        print("delete clicked")

    def on_win_destroy(self, *args):
        print("quit!")


app = Application()
exit_status = app.run(sys.argv)
