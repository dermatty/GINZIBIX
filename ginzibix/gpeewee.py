
from peewee import Model, CharField, ForeignKeyField, IntegerField, TimeField, OperationalError, BooleanField, BlobField, DateTimeField
from playhouse.sqlite_ext import CSqliteExtDatabase
from playhouse.fields import PickleField
from .mplogging import setup_logger, whoami
import os
import shutil
import time
import glob
import re
import zmq
import threading
import signal
import sqlite3
import _pickle as cpickle
import datetime
import functools
import binascii
import pickle
from setproctitle import setproctitle
from dateutil.relativedelta import relativedelta


TERMINATED = False

if __name__ == "__main__":
    from par2lib import calc_file_md5hash, Par2File
else:
    from .par2lib import calc_file_md5hash, Par2File


def lists_are_equal(list1, list2):
    return set(list1) == set(list2) and len(list1) == len(list2)


class DillField(BlobField):
    def python_value(self, value):
        if isinstance(value, (bytearray, sqlite3.Binary)):
            value = bytes(value)
        return cpickle.loads(value)

    def db_value(self, value):
        return sqlite3.Binary(pickle.dumps(value))


class PWDB():
    def __init__(self, cfg, dirs, logger):
        self.logger = logger
        maindir = dirs["main"]
        self.sorted_nzbs_for_gui = None
        self.sorted_nzbshistory_for_gui = None
        self.cfg = cfg
        self.lock = threading.Lock()
        self.last_update_for_gui = datetime.datetime.now()
        self.db_file_name = maindir + "ginzibix.db"
        if os.path.isfile(self.db_file_name):
            self.db_file_exists = True
        else:
            self.db_file_exists = False

        self.old_ts_makeallfilelist = datetime.datetime.now()
        self.db_timestamp = datetime.datetime.now()

        try:
            self.db_file = CSqliteExtDatabase(self.db_file_name)
            self.db = CSqliteExtDatabase(":memory:")

        except Exception as e:
            self.logger.warning(whoami() + str(e))

        ipc_location = maindir + "ginzibix_socket1"
        self.wrapper_context = zmq.Context()
        self.wrapper_socket = self.wrapper_context.socket(zmq.REP)
        self.wrapper_socket.bind("ipc://" + ipc_location)
        self.signal_ign_sigint = None
        self.signal_ign_sigterm = None

        self.current_nzb = None

        class BaseModel(Model):
            class Meta:
                database = self.db

        class TimestampedModel(BaseModel):
            def save(self, *args, **kwargs):
                self.date_updated = datetime.datetime.now()
                return super(TimestampedModel, self).save(*args, **kwargs)

        class MSG(BaseModel):
            nzbname = CharField()
            timestamp = TimeField()
            message = CharField()
            level = CharField()

        class SERVER_TS(BaseModel):
            servername = CharField()
            timestamp = TimeField()
            downloaded = IntegerField()    # in MB

        class NZB(TimestampedModel):
            name = CharField(unique=True)
            priority = IntegerField(default=-1)
            date_updated = DateTimeField(default=datetime.datetime.now())
            timestamp = TimeField()
            # nzb status:
            #    0 ... not queued yet
            #    1 ... nzb processed / queued
            #    2 ... downloading
            #    3 ... download ok / postprocessing
            #    4 ... postprocessing ok, all ok
            #   -1 ... nzb processing failed
            #   -2 ... download failed
            #   -3 ... postproc / unrar etc failed
            #   -4 ... finally failed
            status = IntegerField(default=0)
            # unrar_status:
            #    0 ... unrar not started / idle
            #    1 ... unrar running
            #    2 ... unrar done + success
            #    -1 .. unrar done + failure
            #    -2 .. unrar needs to start from a previous volume
            unrar_status = IntegerField(default=0)
            # verify_status:
            #    0 ... idle / not started
            #    1 ... verifier running
            #    2 ... verifier done + success
            #    -1 .. verifier done + failure
            #    -2 .. verifier running, failure
            preanalysis_status = IntegerField(default=0)
            # preanalysis_status:
            #    0 ... not done
            #    1 ... already done
            verify_status = IntegerField(default=0)
            loadpar2vols = BooleanField(default=False)
            is_pw = BooleanField(default=False)
            is_pw_checked = BooleanField(default=False)
            password = CharField(default="N/A")
            allfilelist_dill = DillField(default="N/A")
            filetypecounter_dill = DillField(default="N/A")
            allfilesizes_dill = DillField(default="N/A")
            p2_dill = DillField(default="N/A")

        class P2(BaseModel):
            fnshort = CharField()
            fnlong = CharField()
            nzbname = CharField()
            p2obj = PickleField()
            rarfiles = PickleField()
            ispw_checked = BooleanField(default=False)
            is_passworded = BooleanField(default=False)
            password = CharField(default="N/A")

        class FILE(BaseModel):
            orig_name = CharField()
            renamed_name = CharField(default="N/A")
            parverify_state = IntegerField(default=0)
            nzb = ForeignKeyField(NZB, backref='files')
            nr_articles = IntegerField(default=0)
            age = IntegerField(default=0)
            ftype = CharField()
            timestamp = TimeField()
            # file status:
            #   0 ... idle
            #   1 ... downloading
            #   2 ... download success
            #   -1 .. download error
            # db_file_update_status(filename, 1)
            status = IntegerField(default=0)
            size = IntegerField(default=0)

        class ARTICLE(BaseModel):
            name = CharField()
            fileentry = ForeignKeyField(FILE, backref='articles')
            size = IntegerField(default=0)
            number = IntegerField(default=0)
            timestamp = TimeField()
            # 0 ... idle
            # 1 ... downloaded
            # -2 .. failed
            status = IntegerField(default=0)

        class SERVERS_TS_SEC(BaseModel):
            servername = CharField()
            # a = pd.timeseries
            # d = a.to_dict()        -> converts timeseries to dic
            # b = pd.DataFrame.from_dict(d, orient="index")

        def max_sql_variables():
            import sqlite3
            db = sqlite3.connect(':memory:')
            cur = db.cursor()
            cur.execute('CREATE TABLE t (test)')
            low, high = 0, 100000
            while (high - 1) > low:
                guess = (high + low) // 2
                query = 'INSERT INTO t VALUES ' + ','.join(['(?)' for _ in
                                                            range(guess)])
                args = [str(i) for i in range(guess)]
                try:
                    cur.execute(query, args)
                except sqlite3.OperationalError as e:
                    if "too many SQL variables" in str(e):
                        high = guess
                    else:
                        raise
                else:
                    low = guess
            cur.close()
            db.close()
            return low

        self.MSG = MSG
        self.NZB = NZB
        self.FILE = FILE
        self.ARTICLE = ARTICLE
        self.SERVER_TS = SERVER_TS
        self.P2 = P2
        self.tablelist = [self.NZB, self.FILE, self.ARTICLE, self.MSG, self.SERVER_TS, self.P2]

        if self.db_file_exists:
            try:
                self.db_file.backup(self.db)
                self.logger.debug(whoami() + "copied file db to :memory: db")
            except Exception:
                self.logger.warning(whoami())
        else:
            self.db.connect()
            self.db.create_tables(self.tablelist)

        self.SQLITE_MAX_VARIABLE_NUMBER = int(max_sql_variables() / 4)

    def set_db_timestamp(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            self.db_timestamp = datetime.datetime.now()
            return func(self, *args, **kwargs)
        return wrapper

    def get_db_timestamp(self):
        return self.db_timestamp

    def db_save_to_file(self):
        try:
            self.db.backup(self.db_file)
            self.logger.info(whoami() + ": saved db to file!")
            return 1
        except Exception as e:
            print(str(e))
            self.logger.error(whoami() + str(e) + ": cannot save db file!")
            return -1

    def do_loop(self):
        self.wrapper_socket.setsockopt(zmq.RCVTIMEO, 1000)
        while not TERMINATED:
            try:
                funcstr, args0, kwargs0 = self.wrapper_socket.recv_pyobj()
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    continue
                else:
                    self.logger.debug(whoami() + str(e) + ": " + funcstr)
                    continue
            except Exception as e:
                self.logger.debug(whoami() + str(e) + ": " + funcstr)
                continue
            ret = eval("self." + funcstr + "(*args0, **kwargs0)")
            try:
                self.wrapper_socket.send_pyobj(ret)
            except Exception as e:
                self.logger.debug(whoami() + str(e) + ": " + funcstr)

    def set_exit_goodbye_from_main(self):
        global TERMINATED
        TERMINATED = True

    def get_all_data_for_gui(self):
        nzb_data = {}
        all_sorted_nzbs = self.db_nzb_getall_sorted_onlydl()
        nzb_data["all#"] = []
        for nzbdata in all_sorted_nzbs:
            n_name, n_prio, n_timestamp, n_status, n_size, n_dlsize = nzbdata
            nzb_data["all#"].append(n_name)
            nzb_data[n_name] = {}
            ispw = self.db_nzb_get_password(n_name)
            pw = self.db_nzb_get_ispw(n_name)
            nzb_data[n_name]["static"] = (n_status, n_prio, n_size, n_dlsize, ispw, pw)
            nzb0 = self.NZB.get(self.NZB.name == n_name)
            nzb_data[n_name]["files"] = {}
            for nzbf in nzb0.files:
                nzb_data[n_name]["files"][nzbf.orig_name] = (nzbf.age, nzbf.ftype, nzbf.nr_articles)
            nzb_data[n_name]["msg"] = self.db_msg_get(n_name)
        return nzb_data

    # ---- self.P2 ----
    def db_p2_insert_p2(self, nzbname, p2, fnshort, fnlong, rarfiles):
        try:
            self.P2.create(nzbname=nzbname, p2obj=p2, fnshort=fnshort, fnlong=fnlong, rarfiles=rarfiles)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            pass

    def db_p2_get_len_p2list(self, nzbname0):
        try:
            p2s0 = self.P2.select().where(self.P2.nzbname == nzbname0)
            return len(p2s0)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return 0

    def db_p2_get_p2list(self, nzbname0):
        try:
            p2s0 = self.P2.select().where(self.P2.nzbname == nzbname0)
            p2list = [(p2s.p2obj, p2s.fnshort, p2s.fnlong, p2s.rarfiles) for p2s in p2s0]
            return p2list
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return []

    def db_p2_set_ispw_checked(self, p2name, ispw_checked):
        try:
            p2 = self.P2.select().where(self.P2.fnshort == p2name)
            p2.ispw_checked = ispw_checked
            p2.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_p2_get_ispw_checked(self, p2name):
        try:
            p2 = self.P2.select().where(self.P2.fnshort == p2name)
            return p2.ispw_checked
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_p2_set_ispw(self, p2name, ispw):
        try:
            p2 = self.P2.select().where(self.P2.fnshort == p2name)
            p2.is_passworded = ispw
            p2.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_p2_get_ispw(self, p2name):
        try:
            p2 = self.P2.select().where(self.P2.fnshort == p2name)
            return p2.is_passworded
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_p2_get_password(self, p2name):
        try:
            p2 = self.P2.select().where(self.P2.fnshort == p2name)
            return p2.password
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_p2_set_password(self, p2name, pw):
        try:
            p2 = self.P2.select().where(self.P2.fnshort == p2name)
            p2.password = pw
            p2.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    # ---- self.SERVER_TS ----
    def db_sts_insert(self, servername, downloaded):
        try:
            self.SERVER_TS.create(servername=servername, timestamp=time.time())
        except Exception:
            pass

    # ---- self.MSG --------
    @set_db_timestamp
    def db_msg_insert(self, nzbname0, msg0, level0, maxitems=5000):
        try:
            new_msg = self.MSG.create(nzbname=nzbname0, timestamp=time.time(), message=msg0, level=level0)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None
        toomuchdata = True
        while toomuchdata:
            try:
                allmsg = self.MSG.select().where(self.MSG.nzbname == nzbname0).order_by(self.MSG.timestamp)
                if len(allmsg) > maxitems:
                    mints = allmsg[0].timestamp
                    query = self.MSG.delete().where(self.MSG.nzbname == nzbname0 and self.MSG.timestamp == mints)
                    query.execute()
                else:
                    toomuchdata = False
            except Exception as e:
                self.logger.warning(whoami() + str(e))
                return None
        self.last_update_for_gui = datetime.datetime.now()
        if new_msg:
            return True
        else:
            return False

    def db_msg_get_last_update(self):
        return self.last_update_for_gui

    def db_msg_get(self, nzbname0):
        msglist = []
        try:
            msg0 = self.MSG.select().where(self.MSG.nzbname == nzbname0).order_by(self.MSG.timestamp.desc())
            msglist = [(msg.message, msg.timestamp, msg.level) for msg in msg0]
            return msglist
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    @set_db_timestamp
    def db_msg_removeall(self, nzbname0):
        try:
            query = self.MSG.delete().where(self.MSG.nzbname == nzbname0)
            query.execute()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    # ---- self.NZB --------
    def db_nzb_get_nzbobj(self, nzbname):
        if self.current_nzb:
            return self.current_nzb
        try:
            nzb = self.NZB.get(self.NZB.name == nzbname)
            return nzb
        except Exception:
            return None

    def db_nzb_get_current_nzbname(self):
        try:
            return self.current_nzb.name
        except Exception:
            return None

    def db_nzb_get_preanalysis_status(self, nzbname):
        nzb0 = self.db_nzb_get_nzbobj(nzbname)
        try:
            return nzb0.preanalysis_status
        except Exception:
            return None

    def db_nzb_set_preanalysis_status(self, nzbname, newstatus):
        nzb = self.db_nzb_get_nzbobj(nzbname)
        try:
            nzb.preanalysis_status = newstatus
            nzb.date_updated = datetime.datetime.now()
            nzb.save()
            return 1
        except Exception:
            return -1

    def db_nzb_are_all_nzb_idle(self):
        try:
            if self.NZB.select().where(self.NZB.status.between(1, 3)).count() == 0:
                return True
            return False
        except Exception:
            return True

    def db_nzb_getnextnzb_for_download(self):
        try:
            nzb = self.NZB.select().where((self.NZB.status == 1) | (self.NZB.status == 2)
                                          | (self.NZB.status == 3)).order_by(self.NZB.priority)[0]
        except Exception:
            return None
        return nzb.name

    @set_db_timestamp
    def db_nzb_undo_postprocess(self, nzbname):
        try:
            # set unrar_status to 0 (idle)
            # set verify_status to 0 (idle)
            # set nzb status to 3 (downloaded/postprocessing)
            nzb = self.NZB.get(self.NZB.name == nzbname)
            self.logger.debug(whoami() + "undo postprocess for nzb " + nzbname)
            nzb.unrar_status = 0
            nzb.verify_status = 0
            nzb.status = 3
            nzb.date_updated = datetime.datetime.now()
            nzb.save()
            # for all rarfiles, set parverify_state to 0
            query = self.FILE.update(parverify_state=0).where((self.FILE.nzb == nzb) & (self.FILE.ftype == "rar"))
            query.execute()
            return True
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    @set_db_timestamp
    def db_nzb_store_allfile_list(self, nzbname, allfilelist, filetypecounter, overall_size, overall_size_wparvol,
                                  p2, usefasttrack=True):
        if not nzbname:
            return False
        try:
            if not usefasttrack:
                nzb = self.NZB.get(self.NZB.name == nzbname)
            else:
                nzb = self.db_nzb_get_nzbobj(nzbname)
            nzb.allfilelist_dill = allfilelist
            nzb.filetypecounter_dill = filetypecounter
            nzb.date_updated = datetime.datetime.now()
            already_downloaded_size = self.calc_already_downloaded_size(nzbname)
            allfilesizes0 = (overall_size, overall_size_wparvol, already_downloaded_size)
            nzb.allfilesizes_dill = allfilesizes0
            nzb.p2_dill = p2
            nzb.save()
        except Exception as e:
            self.logger.debug(whoami() + str(e))
            return False

    def db_nzb_get_allfile_list(self, nzbname):
        try:
            nzb = self.NZB.get(self.NZB.name == nzbname)
        except Exception as e:
            self.logger.debug(whoami() + str(e))
            return None
        allfilelist = nzb.allfilelist_dill
        filetypecounter = nzb.filetypecounter_dill
        p2 = nzb.p2_dill
        allfilesizes0 = nzb.allfilesizes_dill
        if allfilesizes0 != "N/A":
            overall_size, overall_size_wparvol, already_downloaded_size = allfilesizes0
        else:
            return None
        # check here if all dillfields != "N/A"
        if allfilelist != "N/A" and filetypecounter != "N/A" and p2 != "N/A":
            return allfilelist, filetypecounter, overall_size, overall_size_wparvol, already_downloaded_size, p2
        return None

    @set_db_timestamp
    def db_nzb_insert(self, name0):
        try:
            prio = max([n.priority for n in self.NZB.select().order_by(self.NZB.priority)]) + 1
        except ValueError:
            prio = 1
        try:
            new_nzb = self.NZB.create(name=name0, priority=prio, timestamp=time.time())
            new_nzbname = new_nzb.name
        except Exception as e:
            new_nzbname = None
            self.logger.warning(whoami() + str(e))
        return new_nzbname

    @set_db_timestamp
    def db_nzb_delete(self, nzbname):
        try:
            nzb0 = self.NZB.get(self.NZB.name == nzbname)
            files = self.FILE.select().where(self.FILE.nzb == nzb0)
            for f0 in files:
                query_articles = self.ARTICLE.delete().where(self.ARTICLE.fileentry == f0)
                query_articles.execute()
                f0.delete_instance()
            nzb0.delete_instance()
            query_msg = self.MSG.delete().where(self.MSG.nzbname == nzbname)
            query_msg.execute()
            query_p2 = self.P2.delete().where(self.P2.nzbname == nzbname)
            query_p2.execute()
            return True
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    def db_nzb_loadpar2vols(self, nzbname):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.loadpar2vols
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    @set_db_timestamp
    def db_nzb_set_current_nzbobj(self, nzbname):
        if not nzbname:
            self.current_nzb = None
        else:
            try:
                self.current_nzb = self.NZB.get(self.NZB.name == nzbname)
            except Exception as e:
                self.logger.debug(whoami() + str(e))
                self.current_nzb = None

    @set_db_timestamp
    def db_nzb_update_loadpar2vols(self, nzbname, lp2):
        nzb = self.db_nzb_get_nzbobj(nzbname)
        nzb.loadpar2vols = lp2
        nzb.date_updated = datetime.datetime.now()
        nzb.save()

    def db_nzb_getsize(self, nzb0, checkpar2volsloaded=False):
        # if checkpar2volsloaded: if par2vols are loaded -> returns size incl. par2vols else without
        try:
            overallsize = sum([self.db_file_getsize(a.orig_name) for a in nzb0.files])
            if checkpar2volsloaded and nzb0.loadpar2vols:
                return overallsize
            elif checkpar2volsloaded and not nzb0.loadpar2vols:
                par2size = sum([self.db_file_getsize(a.orig_name) for a in nzb0.files if a.ftype == "par2vol"])
                return overallsize - par2size
            else:
                return overallsize
        except Exception as e:
            self.logger.debug(whoami() + str(e))

    def db_nzb_get_downloadedsize(self, nzb0):
        try:
            if nzb0.status not in [0, 1]:
                size = sum([self.db_file_get_downloadedsize(nzbfile) for nzbfile in nzb0.files])
            else:
                size = 0
            return size
        except Exception:
            return 0

    def db_nzb_exists(self, name):
        try:
            nzb = self.NZB.get(self.NZB.name == name)
            assert(nzb.name)
            return True
        except Exception:
            return False

    @set_db_timestamp
    def db_nzb_deleteall(self):
        query = self.NZB.delete()
        query.execute()

    def db_nzb_getall(self):
        nzbs = []
        for n in self.NZB.select():
            nzbs.append((n.name, n.priority, n.timestamp, n.status))
        return nzbs

    def db_nzb_getall_sorted_onlydl(self):
        try:
            query = self.NZB.select().order_by(+self.NZB.priority)
            nzblist = [(n.name, n.priority, n.timestamp, n.status, self.db_nzb_getsize(n, checkpar2volsloaded=True),
                        self.db_nzb_get_downloadedsize(n))
                       for n in query if n.status in [1, 2, 3]]
            return nzblist
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return []

    def db_nzb_getall_sorted(self):
        try:
            query = self.NZB.select().order_by(+self.NZB.priority)
            nzblist = [(n.name, n.priority, n.timestamp, n.status, self.db_nzb_getsize(n, checkpar2volsloaded=True),
                        self.db_nzb_get_downloadedsize(n))
                       for n in query if n.status in [1, 2, 3]]
            query = self.NZB.select().order_by(-self.NZB.date_updated)
            nzblist_history = [(n.name, n.priority, n.date_updated, n.status, self.db_nzb_getsize(n, checkpar2volsloaded=True),
                                self.db_nzb_get_downloadedsize(n))
                               for n in query if n.status in [4, -1, -2, -3, -4, -5]]
            return nzblist, nzblist_history
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return [], []

    @set_db_timestamp
    def db_nzb_set_password(self, nzbname, pw):
        nzb = self.db_nzb_get_nzbobj(nzbname)
        nzb.password = pw
        nzb.date_updated = datetime.datetime.now()
        nzb.save()

    def db_nzb_get_password(self, nzbname):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.password
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    @set_db_timestamp
    def db_nzb_set_ispw(self, nzbname, ispw):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            nzb.is_pw = ispw
            nzb.date_updated = datetime.datetime.now()
            nzb.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_nzb_get_ispw(self, nzbname):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.is_pw
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    @set_db_timestamp
    def db_nzb_set_ispw_checked(self, nzbname, ispw_checked):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            nzb.is_pw_checked = ispw_checked
            nzb.date_updated = datetime.datetime.now()
            nzb.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_nzb_get_ispw_checked(self, nzbname):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.is_pw_checked
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    @set_db_timestamp
    def db_nzb_update_unrar_status(self, nzbname, newstatus):
        nzb = self.db_nzb_get_nzbobj(nzbname)
        if nzb:
            nzb.unrar_status = newstatus
            nzb.date_updated = datetime.datetime.now()
            nzb.save()

    def db_nzb_update_verify_status(self, nzbname, newstatus):
        nzb = self.db_nzb_get_nzbobj(nzbname)
        if nzb:
            nzb.verify_status = newstatus
            nzb.date_updated = datetime.datetime.now()
            nzb.save()

    @set_db_timestamp
    def db_nzb_update_status(self, nzbname, newstatus, usefasttrack=True):
        try:
            if not usefasttrack:
                nzb = self.NZB.get(self.NZB.name == nzbname)
            else:
                nzb = self.db_nzb_get_nzbobj(nzbname)
            nzb.status = newstatus
            nzb.date_updated = datetime.datetime.now()
            nzb.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_nzb_getstatus(self, nzbname, nzb0=None):
        if nzb0:
            return nzb0.status
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.status
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_nzb_get_unrarstatus(self, nzbname):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.unrar_status
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    # return:
    #         -1 - checksum check failed
    #          0 - no sfv file!
    #          1 - checksum ok
    def db_nzb_check_sfvcrc32(self, nzbname, renamed_dir, file_to_be_checked):
        try:
            nzb0 = self.db_nzb_get_nzbobj(nzbname)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return 0
        if nzb0:
            try:
                f0 = self.FILE.get(self.FILE.ftype == "sfv", self.FILE.nzb == nzb0)
                f0list = [f00.renamed_name for f00 in f0]
            except Exception:
                try:
                    f0list = [f0.renamed_name]
                except Exception:
                    return 0
            if f0list:
                # calc crc32 of file
                try:
                    buf = open(file_to_be_checked, 'rb').read()
                    buf = (binascii.crc32(buf) & 0xFFFFFFFF)
                    crc32 = str("%08X" % buf).lower().strip()
                    fileshort = file_to_be_checked.split("/")[-1]
                except Exception:
                    return 0
                crc32ff = None
                for ff in f0list:
                    try:
                        sfvf = open(renamed_dir + ff, "r")
                        for svfline in sfvf:
                            if not svfline.startswith(";") and fileshort in svfline:
                                crc32ff = svfline.split(" ")[-1].lower().strip()
                                break
                        sfvf.close()
                    except Exception:
                        pass
                    if crc32ff:
                        break
                if crc32ff == crc32:
                    return 1
                else:
                    return -1
            return 0
        else:
            return 0

    def db_nzb_get_verifystatus(self, nzbname):
        try:
            nzb = self.db_nzb_get_nzbobj(nzbname)
            return nzb.verify_status
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_nzbname_to_nzbentry(self, nzbname):
        try:
            nzbentry = self.NZB.select().where(self.NZB.name == nzbname)[0]
            return nzbentry
        except Exception as e:
            self.logger.debug(whoami() + str(e))
            return None

    # ---- self.FILE --------
    def db_file_get_renamed(self, name):
        try:
            f0 = self.FILE.get(self.FILE.renamed_name == name)
            return (f0.orig_name, f0.renamed_name, f0.ftype)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_get_renamed_name(self, origname):
        try:
            f0 = self.FILE.get(self.FILE.orig_name == origname)
            return f0.renamed_name
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_getftype_renamed(self, name):
        try:
            file0 = self.FILE.get(self.FILE.renamed_name == name)
            return file0.ftype
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_getsize(self, name):
        try:
            file0 = self.FILE.get(self.FILE.orig_name == name)
            return file0.size
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return 0

    def db_file_get_downloadedsize(self, file0):
        if file0.status == 1:
            return sum([a.size for a in file0.articles if a.status != 0])
        elif file0.status in [2, -1]:
            return file0.size
        else:
            return 0

    def db_file_getsize_renamed(self, name):
        file0 = self.FILE.get(self.FILE.renamed_name == name)
        return file0.size

    def db_allnonrarfiles_getstate(self, nzbname):
        try:
            files00 = self.FILE.select()
            files0 = [f0 for f0 in files00 if f0.nzb.name == nzbname]
            statusok = True
            for f0 in files0:
                if f0.ftype not in ["rar", "par2", "par2vol"] and f0.status <= 0:
                    self.logger.info(whoami() + f0.orig_name + " / " + str(f0.status))
                    statusok = False
                    # break
            return statusok
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    def db_get_all_ok_nonrarfiles(self, nzbname):
        # files0 = self.FILE.get(self.FILE.nzb.name == nzbname)
        files0 = self.FILE.select()
        res0 = [f0 for f0 in files0 if f0.ftype not in ["rar", "par2", "par2vol"] and f0.status > 0 and f0.nzb.name == nzbname]
        return res0

    def get_all_renamed_rar_files(self, nzbname):
        try:
            rarfiles = [f0 for f0 in self.NZB.get(self.NZB.name == nzbname).files if f0.ftype == "rar" and f0.parverify_state == 0 and f0.renamed_name != "N/A"]
            rarflist = [(r.renamed_name, r.orig_name) for r in rarfiles]
            return rarflist
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def get_all_corrupt_rar_files(self, nzbname):
        try:
            rarfiles = [(f0.orig_name, f0.renamed_name) for f0 in self.NZB.get(self.NZB.name == nzbname).files
                        if f0.ftype == "rar" and f0.parverify_state == -1 and f0.renamed_name != "N/A"]
            return rarfiles
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_only_failed_or_ok_rars(self, nzbname):
        try:
            rarfiles = [rf for rf in self.NZB.get(self.NZB.name == nzbname).files if rf.ftype == "rar"]
            # rarfiles = self.FILE.select().where(self.FILE.ftype == "rar")
            rarstates = [r.parverify_state for r in rarfiles]
            if (0 not in rarstates) and (-1 not in rarstates):
                return True
            return False
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_only_verified_rars(self, nzbname):
        try:
            nzbfiles = self.NZB.get(self.NZB.name == nzbname).files
            rarfiles = [rf for rf in nzbfiles if rf.ftype == "rar"]
            rarl = [(r.parverify_state, r.orig_name) for r in rarfiles]
            rarstates = [r.parverify_state for r in rarfiles]
            if (0 not in rarstates):
                return True, rarl
            return False, rarl
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None, []

    def get_renamed_p2(self, dir01, nzbname):
        try:
            par2file0list = [nf for nf in self.NZB.get(self.NZB.name == nzbname).files if nf.ftype == "par2"]
            if par2file0list:
                par2file0 = par2file0list[0]
            else:
                raise ValueError("multiple par2 files appeared or no par2files: " + str(par2file0list))
            if par2file0.renamed_name != "N/A":
                self.logger.debug(whoami() + "got par2 file: " + par2file0.renamed_name)
                p2 = Par2File(dir01 + par2file0.renamed_name)
                return p2
            else:
                return None
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_get_renamed_par2(self, nzbname):
        try:
            par2list = [nf for nf in self.NZB.get(self.NZB.name == nzbname).files if nf.ftype == "par2" and nf.renamed_name != "N/A"]
            if par2list:
                par2 = par2list[0]
            else:
                raise ValueError("multiple par2 files appeared or no par2files: " + str(par2list))
            return par2.renamed_name
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    @set_db_timestamp
    def db_file_update_status(self, filename, newstatus):
        try:
            query = self.FILE.update(status=newstatus).where(self.FILE.orig_name == filename)
            query.execute()
            file0 = self.FILE.get((self.FILE.orig_name == filename))
            file0.nzb.date_updated = datetime.datetime.now()
            file0.nzb.save()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    @set_db_timestamp
    def db_file_update_parstatus(self, filename, newparstatus):
        query = self.FILE.update(parverify_state=newparstatus).where(self.FILE.orig_name == filename)
        query.execute()

    @set_db_timestamp
    def db_file_update_size(self, filename, fsize):
        query = self.FILE.update(size=fsize).where(self.FILE.orig_name == filename)
        query.execute()

    @set_db_timestamp
    def db_file_update_nzbstatus(self, filename0, newnzbstatus0):
        file0 = self.FILE.get((self.FILE.orig_name == filename0))
        file0.nzb.status = newnzbstatus0
        file0.save()

    @set_db_timestamp
    def db_file_set_renamed_name(self, orig_name0, renamed_name0):
        try:
            query = self.FILE.update(renamed_name=renamed_name0).where(self.FILE.orig_name == orig_name0)
            query.execute()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    @set_db_timestamp
    def db_file_set_file_type(self, orig_name0, ftype0):
        try:
            query = self.FILE.update(ftype=ftype0).where(self.FILE.orig_name == orig_name0)
            query.execute()
            return 1
        except Exception:
            return -1

    def db_fname_to_fentry(self, fname):
        try:
            fentry = self.FILE.select().where(self.FILE.orig_name == fname)[0]
            return fentry
        except Exception as e:
            self.logger.debug(whoami() + str(e))
            return None

    def db_file_getstatus(self, filename):
        try:
            query = self.FILE.get(self.FILE.orig_name == filename)
            return query.status
        except Exception:
            pass
        try:
            query = self.FILE.get(self.FILE.renamed_name == filename)
            return query.status
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_getparstatus(self, filename):
        try:
            query = self.FILE.get(self.FILE.renamed_name == filename)
            return query.parverify_state
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_get_orig_filetype(self, filename):
        try:
            query = self.FILE.get(self.FILE.orig_name == filename)
            return query.ftype
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_getallparstatus(self, state):
        try:
            filesparstatus = [f.parverify_state for f in self.FILE.select() if f.parverify_state == state and f.ftype == "rar"]
            return filesparstatus
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return [-9999]

    @set_db_timestamp
    def db_file_insert(self, name, nzbname, nr_articles, age, ftype):
        try:
            nzb0 = self.db_nzbname_to_nzbentry(nzbname)
            file0 = self.FILE.create(orig_name=name, nzb=nzb0, nr_articles=nr_articles, age=age, ftype=ftype, timestamp=time.time())
            new_file = file0.orig_name
        except Exception as e:
            new_file = None
            self.logger.warning(whoami() + str(e))
        return new_file

    def db_file_getall(self):
        files = []
        for f in self.FILE.select():
            files.append((f.orig_name, f.renamed_name, f.ftype, f.nzb.name, f.timestamp, f.status, f.parverify_state,
                          f.age, f.nr_articles, f.status))
        return files

    @set_db_timestamp
    def db_file_deleteall(self):
        query = self.FILE.delete()
        query.execute()

    # ---- self.ARTICLE --------
    @set_db_timestamp
    def db_article_insert(self, name, fileentry, size, number):
        try:
            new_article = self.ARTICLE.create(name=name, fileentry=fileentry, timestamp=time.time())
        except Exception as e:
            new_article = None
        return new_article

    def db_article_getall(self):
        articles = []
        for a in self.ARTICLE.select():
            articles.append((a.name, a.fileentry, a.timestamp, a.status))
        return articles

    @set_db_timestamp
    def db_article_set_status(self, artnamelist, newstatus):
        chunksize = self.SQLITE_MAX_VARIABLE_NUMBER
        llen = len(artnamelist)
        i = 0
        while i < llen:
            artnamelist0 = artnamelist[i: min(i + chunksize, llen)]
            try:
                query = self.ARTICLE.update(status=newstatus).where(self.ARTICLE.name.in_(artnamelist0))
                query.execute()
            except OperationalError as e:
                chunksize = int(chunksize * 0.9)
                continue
            except Exception as e:
                self.logger.warning(whoami() + str(e))
            i += chunksize

    def db_article_get_status(self, artname):
        article0 = self.ARTICLE.get(self.ARTICLE.name == artname)
        if not article0:
            return 0
        return article0.status

    @set_db_timestamp
    def db_article_deleteall(self):
        query = self.ARTICLE.delete()
        query.execute()

    @set_db_timestamp
    def db_article_insert_many(self, data):
        i = 0
        chunksize = self.SQLITE_MAX_VARIABLE_NUMBER
        llen = len(data)
        while i < llen:
            data0 = data[i: min(i + chunksize, llen)]
            for j, (a_aname, a_fname, a_size, a_no, a_ts) in enumerate(data0):
                data0[j] = (a_aname, self.db_fname_to_fentry(a_fname), a_size, a_no, a_ts)
            try:
                query = self.ARTICLE.insert_many(data0, fields=[self.ARTICLE.name, self.ARTICLE.fileentry, self.ARTICLE.size, self.ARTICLE.number,
                                                                self.ARTICLE.timestamp])
                query.execute()
            except OperationalError:
                chunksize = int(chunksize * 0.9)
                continue
            except Exception as e:
                self.logger.warning(whoami() + str(e))
            i += chunksize
        self.SQLITE_MAX_VARIABLE_NUMBER = chunksize

    # ---- self.DB --------
    @set_db_timestamp
    def db_close(self):
        self.db_file.close()
        self.db.close()

    @set_db_timestamp
    def db_drop(self):
        self.db.drop_tables(self.tablelist)

    # ---- move nzbs to history / invert status acc. to nzb list ----
    @set_db_timestamp
    def move_nzb_list(self, new_nzb_list, move_and_resetprios=False):
        try:
            old_nzb_list = [n.name for n in self.NZB.select().where(self.NZB.status.between(1, 3)).order_by(self.NZB.priority)]
            oldfirstnzbname = old_nzb_list[0]
        except Exception:
            oldfirstnzbname = None

        # if nothing changed if old and new are empty return False
        if (new_nzb_list == old_nzb_list) or (not new_nzb_list and not old_nzb_list):
            return False, []

        try:
            newfirstnzbname = new_nzb_list[0]
        except Exception:
            newfirstnzbname = None
            pass

        # has first nzb changed?
        if oldfirstnzbname != newfirstnzbname:
            first_has_changed = True
        else:
            first_has_changed = False

        # get list of moved nzbs & invert their status
        moved_nzbs = []
        for oldnzbname in old_nzb_list:
            if oldnzbname not in new_nzb_list:
                moved_nzbs.append(oldnzbname)
                if move_and_resetprios:
                    oldstatus = self.db_nzb_getstatus(oldnzbname)
                    if oldstatus:
                        newstatus = abs(oldstatus) * -1
                        self.db_nzb_update_status(oldnzbname, newstatus)

        # if we just want to get information, return info
        if not move_and_resetprios:
            return first_has_changed, moved_nzbs

        # now reset prios according to new_nzb_list
        prio = 1
        for newnzb in new_nzb_list:
            if newnzb not in old_nzb_list:
                continue
            else:
                old_nzb_list.remove(newnzb)     # check: old_nzb_list must be empty finally
            try:
                query = self.NZB.update(priority=prio, date_updated=datetime.datetime.now()).where(self.NZB.name == newnzb)
                query.execute()
                prio += 1
            except Exception as e:
                self.logger.error(whoami() + str(e) + ": cannot set new NZB prio!")

        if old_nzb_list:
            self.logger.error(whoami() + "new NZB list is not a subset of old one!")

        return first_has_changed, moved_nzbs

    # ---- set prio of nzb to second in list, nzb must not be in state [1 .. 3] yet! ----
    @set_db_timestamp
    def nzb_prio_insert_second(self, nzbname, newstatus):
        try:
            old_nzb_list = [n.name for n in self.NZB.select().where(self.NZB.status.between(1, 3)).order_by(self.NZB.priority)]
        except Exception:
            old_nzb_list = []
        # if now nzbs yet in [1 .. 3] -> prio 1!
        if not old_nzb_list:
            query = self.NZB.update(priority=1, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
        # else insert prio as second
        else:
            prio = 1
            for nzb in old_nzb_list:
                query = self.NZB.update(priority=1, date_updated=datetime.datetime.now()).where(self.NZB.name == nzb)
                query.execute()
                if prio == 1:
                    prio += 2
                else:
                    prio += 1
            query = self.NZB.update(priority=2, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
        self.db_nzb_update_status(nzbname, newstatus)
        return

    # ---- set new prios acc. to nzb list ----
    @set_db_timestamp
    def reorder_nzb_list(self, new_nzb_list, delete_and_resetprios=False):
        # assumption: new_nzb_list is a (possibly empty) subset of old_nzb_list
        # otherwise this likely will produce nonsense
        try:
            old_nzb_list = [n.name for n in self.NZB.select().where(self.NZB.status.between(1, 3)).order_by(self.NZB.priority)]
            oldfirstnzbname = old_nzb_list[0]
        except Exception:
            oldfirstnzbname = None

        # if nothing changed if old and new are empty return False
        if (new_nzb_list == old_nzb_list) or (not new_nzb_list and not old_nzb_list):
            return False, []

        try:
            newfirstnzbname = new_nzb_list[0]
        except Exception:
            newfirstnzbname = None
            pass

        # has first nzb changed?
        if oldfirstnzbname != newfirstnzbname:
            first_has_changed = True
        else:
            first_has_changed = False

        # get list of deleted nzbs & delete nzbs from db which are in old_nzb_list but not in new one
        deleted_nzbs = []
        for oldnzbname in old_nzb_list:
            if oldnzbname not in new_nzb_list:
                deleted_nzbs.append(oldnzbname)
                if delete_and_resetprios:
                    self.db_nzb_delete(oldnzbname)

        # if we just want to get information, return info
        if not delete_and_resetprios:
            return first_has_changed, deleted_nzbs

        # now reset prios according to new_nzb_list
        prio = 1
        for newnzb in new_nzb_list:
            if newnzb not in old_nzb_list:
                continue
            else:
                old_nzb_list.remove(newnzb)     # check: old_nzb_list must be empty finally
            try:
                query = self.NZB.update(priority=prio, date_updated=datetime.datetime.now()).where(self.NZB.name == newnzb)
                query.execute()
                prio += 1
            except Exception as e:
                self.logger.error(whoami() + str(e) + ": cannot set new NZB prio!")

        if old_nzb_list:
            self.logger.error(whoami() + "new NZB list is not a subset of old one!")

        return first_has_changed, deleted_nzbs

    # ---- send sorted nzbs to guiconnector ---
    def get_stored_sorted_nzbs(self):
        return self.sorted_nzbs_for_gui

    def get_stored_sorted_nzbhistory(self):
        return self.sorted_nzbshistory_for_gui

    def store_sorted_nzbs(self):
        sortednzbs, sortednzbs_history = self.db_nzb_getall_sorted()
        if sortednzbs == []:
            sortednzbs = [-1]
        if sortednzbs_history == []:
            sortednzbs_history = [-1]
        self.sorted_nzbs_for_gui = sortednzbs
        self.sorted_nzbshistory_for_gui = sortednzbs_history

    # ---- log info for nzb in db ###
    def log(self, nzbname, logmsg, loglevel, logger):
        logger.info(logmsg)
        self.db_msg_insert(nzbname, logmsg, loglevel)

    # ---- get_downloaded_file_full_path ----
    def get_downloaded_file_full_path(self, file0, dir0):
        file_already_exists = False
        # self.logger.info(whoami() + dir0 + "*")
        for fname0 in glob.glob(dir0 + "*") + glob.glob(dir0 + ".*"):
            short_fn = fname0.split("/")[-1]
            if short_fn == file0.orig_name or short_fn == file0.renamed_name:
                file_already_exists = True
                break
        return dir0 + file0.orig_name, file_already_exists

    @set_db_timestamp
    def nzb_reset(self, nzbname, incompletedir, nzbdir):
        # delete nzb + files + articles in db
        self.db_nzb_delete(nzbname)
        # delete incomplete_dir
        self.logger.warning(whoami() + "inconsistency in verified rars, deleting incomplete_dir")
        try:
            if os.path.isdir(incompletedir):
                self.logger.info(whoami() + "Removing incomplete_dir")
                shutil.rmtree(incompletedir)
        except Exception as e:
            self.logger.error(whoami() + str(e) + ": error in removing incomplete_dir")
        # rename nzbfile in order to provoque re-read by nzb-parser
        oldnzb = nzbdir + nzbname
        newnzb = nzbdir + nzbname + "(1)"
        try:
            os.rename(oldnzb, newnzb)
        except Exception as e:
            self.logger.error(whoami() + str(e) + ": error in renaming nzb")

    def calc_already_downloaded_size(self, nzbname):
        gbdivisor = (1024 * 1024 * 1024)
        try:
            nzb = self.NZB.get(self.NZB.name == nzbname)
            files = [file0 for file0 in nzb.files]
        except Exception as e:
            self.logger.debug(whoami() + str(e))
            return 0
        return sum([self.db_file_get_downloadedsize(f) for f in files]) / gbdivisor

    @set_db_timestamp
    def create_allfile_list_via_name(self, nzbname, dir0):
        try:
            nzb = self.NZB.get(self.NZB.name == nzbname)
            return self.create_allfile_list(nzb, dir0)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None, None

    @set_db_timestamp
    def create_allfile_list(self, nzb, dir0):
        nzbname = nzb.name
        gbdivisor = (1024 * 1024 * 1024)
        res = self.db_nzb_get_allfile_list(nzbname)
        if res:
            allfilelist, filetypecounter, overall_size, overall_size_wparvol, _, p2 = res
            return filetypecounter, nzbname
        allfilelist = []
        filetypecounter = {"rar": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "nfo": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "par2": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "par2vol": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "sfv": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "etc": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []}}
        dir00 = dir0 + "_downloaded0/"
        dir01 = dir0 + "_renamed0/"
        files = [files0 for files0 in nzb.files]   # if files0.status in [0, 1]]
        if not files:
            self.logger.info(whoami() + "No files to download for NZB " + nzb.name)
            return None, None
        idx = 0
        overall_size = 0
        overall_size_wparvol = 0
        already_downloaded_size = 0
        p2 = None
        for f0 in files:
            articles = [articles0 for articles0 in f0.articles]
            f0size = sum([a.size for a in articles])
            if f0.ftype == "par2vol":
                overall_size_wparvol += f0size
            else:
                overall_size += f0size
            filetypecounter[f0.ftype]["max"] += 1
            filetypecounter[f0.ftype]["filelist"].append(f0.orig_name)
            self.logger.info(whoami() + f0.orig_name + ", status in db: " + str(f0.status) + ", filetype: " + f0.ftype)
            if f0.status == 2:
                full_filename_renamed = dir01 + f0.renamed_name
                full_filename_downloaded = dir00 + f0.renamed_name
                # self.logger.debug(whoami() + " >>> " + full_filename_renamed)
                # self.logger.debug(whoami() + " >>> " + full_filename_downloaded)
                filename0 = None
                if f0.ftype == "par2":
                    if os.path.isfile(full_filename_renamed):
                        p2 = Par2File(full_filename_renamed)
                        self.logger.debug(whoami() + "par2 found: " + full_filename_renamed)
                        filename0 = full_filename_renamed
                    elif os.path.isfile(full_filename_downloaded):
                        p2 = Par2File(full_filename_downloaded)
                        self.logger.debug(whoami() + "par2 found: " + full_filename_downloaded)
                        filename0 = full_filename_downloaded
                    else:
                        self.logger.error("Processing par2 file, but not found in dirs; this should not occur - will download again!!")
                        # return None, None, None, None, None, None, None, None
                else:
                    if os.path.isfile(full_filename_renamed):
                        filename0 = full_filename_renamed
                    elif os.path.isfile(full_filename_downloaded):
                        filename0 = full_filename_downloaded
                if not filename0:
                    self.logger.warning(whoami() + "processing " + f0.orig_name + ", but not found in dirs; this should not occur - will download again!!")
                else:
                    filetypecounter[f0.ftype]["counter"] += 1
                    md5 = calc_file_md5hash(filename0)
                    filetypecounter[f0.ftype]["loadedfiles"].append((f0.orig_name, filename0, md5))
                    already_downloaded_size += f0size
                    continue
            allfilelist.append([(f0.orig_name, f0.age, f0.ftype, f0.nr_articles)])
            articles = [articles0 for articles0 in f0.articles if articles0.status in [0, 1]]
            for a in articles:
                allok = True
                if len(allfilelist[idx]) > 2:
                    for i1, art in enumerate(allfilelist[idx]):
                        if i1 > 1:
                            nr1, fn1, _ = art
                            if nr1 == a.number:
                                allok = False
                                break
                if allok:
                    allfilelist[idx].append((a.number, a.name, a.size))
            idx += 1
        if allfilelist:
            self.db_nzb_update_status(nzbname, 1, usefasttrack=False)
            overall_size /= gbdivisor
            overall_size_wparvol /= gbdivisor
            overall_size_wparvol += overall_size
            already_downloaded_size /= gbdivisor
            # save results in db
            self.db_nzb_store_allfile_list(nzbname, allfilelist, filetypecounter, overall_size,
                                           overall_size_wparvol, p2, usefasttrack=False)
            return filetypecounter, nzbname
        else:
            self.db_nzb_update_status(nzbname, 2, usefasttrack=False)
            return None, None

    # ---- make_allfilelist -------
    #      makes a file/articles list out of top-prio nzb, ready for beeing queued
    #      to download threads
    def make_allfilelist(self, dir0, nzbdir):
        self.clean_db(nzbdir)
        if self.db_timestamp < self.old_ts_makeallfilelist:
            return None
        try:
            nzb = self.NZB.select().where(self.NZB.status.between(1, 3)).order_by(self.NZB.priority)[0]
        except Exception:
            return None
        self.old_ts_makeallfilelist = datetime.datetime.now()
        self.logger.info(whoami() + "analyzing NZB: " + nzb.name + " with status: " + str(nzb.status))
        nzbname = nzb.name
        nzbstatus = self.db_nzb_getstatus(nzbname, nzb)
        nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
        incompletedir = dir0 + nzbdir
        # state "queued"
        if nzbstatus == 1:
            files = [files0 for files0 in nzb.files]   # if files0.status in [0, 1]]
            if not files:
                self.logger.info(whoami() + "No files to download for NZB " + nzb.name)
                self.db_nzb_update_status(nzbname, -1, usefasttrack=False)     # download failed as no files present
                return None
            # if queued and incomplete dir exists -> delete, because of inconsistent state!
            try:
                if os.path.isdir(incompletedir):
                    self.logger.info(whoami() + "Removing incomplete_dir")
                    shutil.rmtree(incompletedir)
            except Exception as e:
                self.logger.error(whoami() + str(e) + ": error in removing incomplete_dir")
                self.db_nzb_update_status(nzbname, -1, usefasttrack=False)     # download failed as no files present
                return None
            # set all files to status "queued" / 0
            for f0 in files:
                self.db_file_update_status(f0.orig_name, 0)
            # loop through files and make allfileslist
            try:
                filetypecounter, nzbame = self.create_allfile_list(nzb, incompletedir)
            except Exception as e:
                self.logger.warning(whoami() + str(e))
            return nzbname

        # state "downloading" / "postprocessing"
        elif nzbstatus in [2, 3]:
            dir_renamed = dir0 + nzbdir + "_renamed0/"
            dir_verified = dir0 + nzbdir + "_verifiedrars0/"
            dir_downloaded = dir0 + nzbdir + "_downloaded0/"
            files = [files0 for files0 in nzb.files]

            # check consistency: verified
            self.logger.debug(whoami() + "checking verified_rars consistency")
            verified_files = [f0.renamed_name for f0 in files if f0.parverify_state == 1]
            rars_in_verified_dir = [fname0.split("/")[-1] for fname0 in glob.glob(dir_verified + "*") + glob.glob(dir_verified + ".*")]
            if not lists_are_equal(verified_files, rars_in_verified_dir):
                self.logger.warning(whoami() + "inconsistency in verified rars, deleting nzb in db")
                self.nzb_reset(nzbname, incompletedir, nzbdir)
                return None
            # check consistency: renamed
            self.logger.debug(whoami() + "verified_dir consistent, checking renamed_dir")
            renamed_files = [f0.renamed_name for f0 in files if f0.renamed_name != "N/A"]
            files_in_renamed_dir = [fname0.split("/")[-1] for fname0 in glob.glob(dir_renamed + "*") + glob.glob(dir_renamed + ".*")]
            files_in_renamed_dir = [f0 for f0 in files_in_renamed_dir if f0[-6:-2] != ".rar."]
            if not lists_are_equal(renamed_files, files_in_renamed_dir):
                self.logger.warning(whoami() + "inconsistency in renamed_dir, deleting nzb in db/filesystem")
                self.nzb_reset(nzbname, incompletedir, nzbdir)
                return None
            # check if all downloaded files exist in _renamed or _downloaded0
            self.logger.debug(whoami() + "renamed_dir consistent, checking if all downloaded files exist")
            files_in_downloaded_dir = [fname0.split("/")[-1] for fname0 in glob.glob(dir_downloaded + "*") + glob.glob(dir_downloaded + ".*")]
            inconsistent = False
            for f0 in files:
                if f0.status == 2:
                    if f0.orig_name not in files_in_downloaded_dir and f0.renamed_name not in files_in_renamed_dir:
                        inconsistent = True
                        break
            if inconsistent:
                self.logger.warning(whoami() + "inconsistency in downloaded files, deleting nzb in db/filesystem")
                self.nzb_reset(nzbname, incompletedir, nzbdir)
                return None
            filetypecounter, nzbame = self.create_allfile_list(nzb, incompletedir)
            if nzbstatus == 2:
                return nzbname
            # if postprocessing: check if all files are downloaded
            # if loadpar2vols == True: check if complete
            if self.db_nzb_loadpar2vols(nzbname):
                if filetypecounter["par2vol"]["max"] > filetypecounter["par2vol"]["counter"]:
                    self.logger.warning(whoami() + "not all par2vol downloaded, deleting nzb in db/filesystem")
                    self.nzb_reset(nzbname, incompletedir, nzbdir)
                    return None
            # check all other filetypepars if complete
            inconsistent = False
            for fset in ["par2", "rar", "sfv", "nfo", "etc"]:
                if filetypecounter[fset]["max"] > filetypecounter[fset]["counter"]:
                    inconsistent = True
                    break
            if inconsistent:
                self.logger.warning(whoami() + "not all files downloaded although in postproc. state, deleting nzb in db/filesystem")
                self.nzb_reset(nzbname, incompletedir, nzbdir)
                return None
            return nzbname

    def clean_db(self, nzbdir):
        now0 = datetime.datetime.now()
        now01m = relativedelta(months=-1) + now0
        nzbs = self.NZB.select().where(self.NZB.date_updated < now01m)
        for nzb in nzbs:
            self.logger.info(whoami() + "Cleaning NZB " + nzb.name + " from DB because older than 1 month")
            deletednzb = nzb.name
            self.db_nzb_delete(deletednzb)
            try:
                os.rename(nzbdir + deletednzb, nzbdir + deletednzb + ".bak")
                self.logger.debug(whoami() + ": renamed NZB " + deletednzb + " to .bak")
            except Exception as e:
                self.logger.warning(whoami() + str(e))
        if nzbs:
            self.db_save_to_file()

    set_db_timestamp = staticmethod(set_db_timestamp)


def wrapper_main(cfg, dirs, mp_loggerqueue):
    setproctitle("gzbx." + os.path.basename(__file__))
    logger = setup_logger(mp_loggerqueue, __file__)

    logger.debug(whoami() + "starting ...")

    pwwt = PWDB(cfg, dirs, logger)
    pwwt.signal_ign_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pwwt.signal_ign_sigterm = signal.signal(signal.SIGTERM, signal.SIG_IGN)

    pwwt.do_loop()
    logger.debug(whoami() + "gpeewee loop joined!")

    try:
        pwwt.db.execute_sql('VACUUM')        # shrink db to minimum size required
        pwwt.db.backup(pwwt.db_file)
        pwwt.db_drop()
        pwwt.db_close()
    except Exception as e:
        print(str(e))
        logger.warning(whoami() + str(e))

    logger.debug(whoami() + "exited!")
