import sys
import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, Gio, Gdk, GdkPixbuf, GLib


__appname__ = "Ginzibix"
__version__ = "0.01 pre-alpha"
__author__ = "dermatty"

GBXICON = "ginzibix.jpe"

MENU_XML = """
<?xml version="1.0" encoding="UTF-8"?>
<interface>
  <menu id="app-menu">
    <section>
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


class AppData:
    def __init__(self):
        self.mbitsec = 0
        # NZB - progress - downloaded - overall - eta
        self.nzbs = [("Fuenf Freunde", 10, 1.21, 2.67, "11m03s", "10%", False),
                     ("Der Gloeckner von Notredame", 1, 0.0, 0.98, "7m14s", "1%", False)]
        for i in range(5):
            self.nzbs.append(self.nzbs[0])


class AppWindow(Gtk.ApplicationWindow):

    def __init__(self, app):
        self.liststore = None
        self.mbitlabel = None
        self.appdata = AppData()
        Gtk.Window.__init__(self, title=__appname__, application=app)
        try:
            self.set_icon_from_file(GBXICON)
        except GLib.GError as e:
            print("Cannot find icon file!")
        # init main window
        self.set_border_width(10)
        # self.set_default_size(600, 200)
        self.set_wmclass(__appname__, __appname__)
        self.header_bar()
        box_main = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        self.add(box_main)

        # stack
        stack = Gtk.Stack()
        stack.set_transition_type(Gtk.StackTransitionType.SLIDE_LEFT_RIGHT)
        stack.set_transition_duration(200)

        self.stacknzb_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        stack.add_titled(self.stacknzb_box, "nzbs", "NZBs")
        self.show_nzb_stack(self.stacknzb_box)

        self.stackdetails_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
        stack.add_titled(self.stackdetails_box, "details", "Details")
        self.stacksettings_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
        stack.add_titled(self.stacksettings_box, "settings", "Settings")
        self.stacklogs_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
        stack.add_titled(self.stacklogs_box, "logs", "Logs")
        stack_switcher = Gtk.StackSwitcher()
        stack_switcher.set_stack(stack)
        stack_switcher.set_property("halign", Gtk.Align.CENTER)
        stack_switcher.set_property("valign", Gtk.Align.START)
        box_main.pack_start(stack_switcher, False, False, 0)
        box_main.pack_start(stack, True, True, 0)

    def show_nzb_stack(self, stacknzb_box):
        # scrolled window
        scrolled_window = Gtk.ScrolledWindow()
        scrolled_window.set_border_width(10)
        scrolled_window.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        scrolled_window.set_property("min-content-height", 300)
        stacknzb_box.pack_start(scrolled_window, True, True, 8)
        # listbox
        listbox = Gtk.ListBox()
        row = Gtk.ListBoxRow()
        # populate liststore
        self.liststore = Gtk.ListStore(str, int, float, float, str, str, bool)
        for i, nzb in enumerate(self.appdata.nzbs):
            if i == 0:
                self.current_iter = self.liststore.append(list(nzb))
            else:
                self.liststore.append(list(nzb))
        # set treeview + actions
        treeview = Gtk.TreeView(model=self.liststore)
        treeview.set_reorderable(True)
        treeview.get_selection().connect("changed", self.on_selection_changed)
        # 1st column: NZB name
        renderer_text0 = Gtk.CellRendererText()
        column_text0 = Gtk.TreeViewColumn("NZB name", renderer_text0, text=0)
        column_text0.set_expand(True)
        treeview.append_column(column_text0)
        # 2nd: progressbar
        renderer_progress = Gtk.CellRendererProgress()
        column_progress = Gtk.TreeViewColumn("Progress", renderer_progress, value=1, text=5)
        column_progress.set_min_width(260)
        column_progress.set_expand(True)
        treeview.append_column(column_progress)
        # 3rd downloaded GiN
        renderer_text1 = Gtk.CellRendererText()
        column_text1 = Gtk.TreeViewColumn("Downloaded", renderer_text1, text=2)
        column_text1.set_cell_data_func(renderer_text1, lambda col, cell, model, iter, unused:
                                        cell.set_property("text", "{0:.2f}".format(model.get(iter, 2)[0]) + " GiB"))
        treeview.append_column(column_text1)
        # 4th overall GiB
        renderer_text2 = Gtk.CellRendererText()
        column_text2 = Gtk.TreeViewColumn("Overall", renderer_text2, text=3)
        column_text2.set_cell_data_func(renderer_text2, lambda col, cell, model, iter, unused:
                                        cell.set_property("text", "{0:.2f}".format(model.get(iter, 3)[0]) + " GiB"))
        column_text2.set_min_width(80)
        treeview.append_column(column_text2)
        # 5th Eta
        renderer_text3 = Gtk.CellRendererText()
        column_text3 = Gtk.TreeViewColumn("Eta", renderer_text3, text=4)
        column_text3.set_min_width(80)
        treeview.append_column(column_text3)
        # 6th selection toggled
        renderer_toggle = Gtk.CellRendererToggle()
        renderer_toggle.connect("toggled", self.on_inverted_toggled)
        column_toggle = Gtk.TreeViewColumn("Select", renderer_toggle, active=6)
        treeview.append_column(column_toggle)
        # final
        row.add(treeview)
        listbox.add(row)
        scrolled_window.add(listbox)
        # grid for record/stop/.. selected
        grid = Gtk.Grid()
        # stacknzb_box.add(grid)
        stacknzb_box.pack_start(grid, True, True, 8)
        button1 = Gtk.Button(label="Button 1")
        button2 = Gtk.Button(label="Button 2")
        button3 = Gtk.Button(label="Button 3")
        button4 = Gtk.Button(label="Button 4")
        button5 = Gtk.Button(label="Button 5")
        button6 = Gtk.Button(label="Button 6")
        grid.add(button1)
        grid.attach(button2, 1, 0, 2, 1)
        grid.attach_next_to(button3, button1, Gtk.PositionType.BOTTOM, 1, 2)
        grid.attach_next_to(button4, button3, Gtk.PositionType.RIGHT, 2, 1)
        grid.attach(button5, 1, 2, 1, 1)
        grid.attach_next_to(button6, button5, Gtk.PositionType.RIGHT, 1, 1)

    def on_inverted_toggled(self, widget, path):
        self.liststore[path][6] = not self.liststore[path][6]

    def on_selection_changed(self, selection):
        (model, iter) = selection.get_selected()
        print(model[iter][0])

    def header_bar(self):
        hb = Gtk.HeaderBar(spacing=20)
        hb.set_show_close_button(True)
        hb.props.title = __appname__
        self.set_titlebar(hb)

        button = Gtk.Button()
        icon = Gio.ThemedIcon(name="media-playback-pause")
        image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
        button.add(image)
        hb.pack_start(button)

        self.mbitlabel = Gtk.Label(None, xalign=0.0, yalign=0.5)
        if self.appdata.mbitsec > 0:
            self.mbitlabel.set_text(str(int(self.appdata.mbitsec)) + " MBit/s")
        else:
            self.mbitlabel.set_text("")
        hb.pack_start(self.mbitlabel)


class Application(Gtk.Application):

    def __init__(self):
        Gtk.Application.__init__(self)
        self.window = None

    def do_activate(self):
        self.window = AppWindow(self)
        self.window.show_all()

    def do_startup(self):
        Gtk.Application.do_startup(self)
        action = Gio.SimpleAction.new("about", None)
        action.connect("activate", self.on_about)
        self.add_action(action)

        action = Gio.SimpleAction.new("quit", None)
        action.connect("activate", self.on_quit)
        self.add_action(action)

        builder = Gtk.Builder.new_from_string(MENU_XML, -1)
        self.set_app_menu(builder.get_object("app-menu"))

    def on_about(self, action, param):
        about_dialog = Gtk.AboutDialog(transient_for=self.window, modal=True)
        about_dialog.set_program_name(__appname__)
        about_dialog.set_version(__version__)
        about_dialog.set_copyright("Copyright \xa9 2018 dermatty")
        about_dialog.set_comments("A binary newsreader for the gnome desktop")
        about_dialog.set_website("https://github.com/dermatty/GINZIBIX")
        about_dialog.set_website_label('Ginzibix on GitHub')
        try:
            about_dialog.set_logo(GdkPixbuf.Pixbuf.new_from_file_at_size(GBXICON, 64, 64))
        except GLib.GError as e:
            print("Cannot find icon file!")

        about_dialog.set_license_type(Gtk.License.GPL_3_0)

        about_dialog.present()

    def on_quit(self, action, param):
        self.quit()


app = Application()
exit_status = app.run(sys.argv)
sys.exit(exit_status)
