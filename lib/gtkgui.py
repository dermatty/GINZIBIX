import sys
import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, Gio, Gdk, GdkPixbuf


__appname__ = "Ginzibix"
__version__ = "0.01 pre-alpha"
__author__ = "dermatty"


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
        self.appname = __appname__


class AppWindow(Gtk.ApplicationWindow):

    def __init__(self, app):
        self.appdata = AppData()
        Gtk.Window.__init__(self, title=self.appdata.appname, application=app)
        self.set_icon_from_file("index.jpe")

        # init main window
        self.set_border_width(10)
        # self.set_default_size(600, 200)
        self.set_wmclass(self.appdata.appname, self.appdata.appname)
        self.header_bar()
        box_main = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        self.add(box_main)

        # stack
        stack = Gtk.Stack()
        stack.set_transition_type(Gtk.StackTransitionType.SLIDE_LEFT_RIGHT)
        stack.set_transition_duration(200)

        self.stacknzb_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=32)
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
        stack_switcher.set_property("valign", Gtk.Align.CENTER)
        box_main.pack_start(stack_switcher, True, True, 0)
        box_main.pack_start(stack, True, True, 0)
        
    def show_nzb_stack(self, stacknzb_box):
        buttonbox = Gtk.Box(spacing=26)
        buttonbox.set_property("halign", Gtk.Align.START)
        buttonbox.set_property("valign", Gtk.Align.CENTER)
        stacknzb_box.pack_start(buttonbox, True, True, 0)
        button1 = Gtk.Button.new_from_icon_name("media-playback-stop", Gtk.IconSize.BUTTON)
        buttonbox.pack_start(button1, True, True, 0)

    def header_bar(self):
        hb = Gtk.HeaderBar(spacing=20)
        hb.set_show_close_button(True)
        hb.props.title = self.appdata.appname
        self.set_titlebar(hb)

        button = Gtk.Button()
        icon = Gio.ThemedIcon(name="media-playback-pause")
        image = Gtk.Image.new_from_gicon(icon, Gtk.IconSize.BUTTON)
        button.add(image)
        hb.pack_start(button)

        hb.pack_start(Gtk.Label(str(int(self.appdata.mbitsec)) + " MBit/s", xalign=0.0, yalign=0.5))


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
        about_dialog.set_copyright("(c) dermatty 2018")
        about_dialog.set_comments("A binary newsreader for the gnome desktop")
        about_dialog.set_website("https://github.com/dermatty/GINZIBIX")
        about_dialog.set_website_label('https://github.com/dermatty/GINZIBIX')
        about_dialog.set_logo(GdkPixbuf.Pixbuf.new_from_file_at_size("index.jpe", 64, 64))

        about_dialog.set_license_type(Gtk.License.GPL_3_0)

        about_dialog.present()

    def on_quit(self, action, param):
        self.quit()


app = Application()
exit_status = app.run(sys.argv)
sys.exit(exit_status)
