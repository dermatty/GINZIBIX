import configparser
import datetime
import signal
import multiprocessing as mp
import logging
import logging.handlers
import gi
import os
import sys
from setproctitle import setproctitle
from ginzibix.mplogging import whoami
from ginzibix import gpeewee, control_loop, gui, mplogging
from ginzibix import is_port_in_use, make_dirs, PWDBSender, __version__, setup_dirs
gi.require_version('Gtk', '3.0')


# Signal handler
class SigHandler_Ginzibix:
    def __init__(self, pwdb, mpp_main, mpp_wrapper, mp_loggerqueue, mp_loglistener, logger):
        self.mpp_main = mpp_main
        self.mpp_wrapper = mpp_wrapper
        self.logger = logger
        self.pwdb = pwdb
        self.mp_loggerqueue = mp_loggerqueue
        self.mp_loglistener = mp_loglistener

    def sighandler_ginzibix(self, a, b):
        self.shutdown()

    def shutdown(self, exit_status=3):
        # wait until main is joined
        if exit_status == 3:
            trstr = str(datetime.datetime.now()) + ": RESTART - "
        else:
            trstr = str(datetime.datetime.now()) + ": SHUTDOWN - "
        # shutdown mpp_main
        if self.mpp_main:
            if self.mpp_main.pid:
                print(trstr + "joining main ...")
                self.mpp_main.join(timeout=30)
                if self.mpp_main.is_alive():
                    print(trstr + "attention, nzb db may be corrupt, killing main")
                    os.kill(self.mpp_main.pid, signal.SIGKILL)
        try:
            self.pwdb.exc("set_exit_goodbye_from_main", [], {})
        except Exception:
            pass
        # shutdown mpp_wrapper
        if self.mpp_wrapper:
            if self.mpp_wrapper.pid:
                print(trstr + "joining mpp_wrapper ...")
                self.mpp_wrapper.join(timeout=5)
                if self.mpp_wrapper.is_alive():
                    print(trstr + "killing mpp_wrapper")
                    os.kill(self.mpp_wrapper.pid, signal.SIGKILL)
        # shutdown loglistener
        if self.mp_loglistener:
            if self.mp_loglistener.pid:
                print(trstr + "joining loglistener ...")
                mplogging.stop_logging_listener(self.mp_loggerqueue, self.mp_loglistener)
                self.mp_loglistener.join(timeout=5)
                if self.mp_loglistener.is_alive():
                    print(trstr + "killing loglistener")
                    os.kill(self.mp_loglistener.pid, signal.SIGKILL)
        print(trstr + "finally done!")


def run():
    setproctitle("gzbx." + os.path.basename(__file__))

    setup_res0, userhome, maindir, dirs, subdirs, gladefile, iconfile = setup_dirs()
    if setup_res0 == -1:
        print(userhome)
        print("Exiting ...")
        sys.exit()

    guiport = 36703
    while is_port_in_use(guiport):
        guiport += 1

    exit_status = 3

    while exit_status == 3:
        
        # init config
        try:
            cfg_file = dirs["config"] + "/ginzibix.config"
            cfg = configparser.ConfigParser()
            cfg.read(cfg_file)
        except Exception as e:
            print(str(e) + ": config file syntax error, exiting")
            sys.exit()

        # init PW file
        if not os.path.isfile(cfg["OPTIONS"]["pw_file"]):
            try:
                pwfile = maindir + "PWFile.txt"
                os.mknod(pwfile)
                cfg["OPTIONS"]["pw_file"] = pwfile
                with open(cfg_file, "w") as cfgfile0:
                    cfg.write(cfgfile0)
            except Exception as e:
                print(str(e) + ": cannot init password file, exiting")
                sys.exit()

        # get log level
        try:
            loglevel_str = cfg["OPTIONS"]["debuglevel"].lower()
            if loglevel_str == "info":
                loglevel = logging.INFO
            elif loglevel_str == "debug":
                loglevel = logging.DEBUG
            elif loglevel_str == "warning":
                loglevel = logging.WARNING
            elif loglevel_str == "error":
                loglevel = logging.ERROR
            else:
                loglevel = logging.INFO
                loglevel_str = "info"
        except Exception:
            loglevel = logging.INFO
            loglevel_str = "info"

        # init logger
        mp_loggerqueue, mp_loglistener = mplogging.start_logging_listener(dirs["logs"] + "ginzibix.log", maxlevel=loglevel)
        logger = mplogging.setup_logger(mp_loggerqueue, __file__)

        logger.debug(whoami() + "starting with loglevel '" + loglevel_str + "'")

        logger.debug(whoami() + "Welcome to GINZIBIX " + __version__)

        # start DB Thread
        mpp_wrapper = mp.Process(target=gpeewee.wrapper_main, args=(cfg, dirs, mp_loggerqueue, ))
        mpp_wrapper.start()

        pwdb = PWDBSender()

        # start main mp
        mpp_main = None
        mpp_main = mp.Process(target=control_loop.ginzi_main, args=(cfg_file, cfg, dirs, subdirs, guiport, mp_loggerqueue, ))
        mpp_main.start()

        # init sighandler
        sh = SigHandler_Ginzibix(pwdb, mpp_main, mpp_wrapper, mp_loggerqueue, mp_loglistener, logger)
        signal.signal(signal.SIGINT, sh.sighandler_ginzibix)
        signal.signal(signal.SIGTERM, sh.sighandler_ginzibix)

        app = gui.ApplicationGui(dirs, cfg, guiport, iconfile, gladefile, mp_loggerqueue)
        exit_status = app.run(sys.argv)

        sh.shutdown(exit_status=exit_status)

    print("ginzibix exits!")
    os._exit(1)
