from peewee import Model, CharField, ForeignKeyField, IntegerField, TimeField, OperationalError, BooleanField, BlobField, DateTimeField
from playhouse.sqlite_ext import CSqliteExtDatabase
import os
import shutil
import time
import glob
import re
import zmq
import threading
import signal
import inspect
import sqlite3
import _pickle as cpickle
import datetime


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


TERMINATED = False

if __name__ == "__main__":
    from par2lib import calc_file_md5hash, Par2File
else:
    from .par2lib import calc_file_md5hash, Par2File

lpref = __name__.split("lib.")[-1] + " - "


def lists_are_equal(list1, list2):
    return set(list1) == set(list2) and len(list1) == len(list2)


class DillField(BlobField):
    def python_value(self, value):
        if isinstance(value, (bytearray, sqlite3.Binary)):
            value = bytes(value)
        return cpickle.loads(value)

    def db_value(self, value):
        return sqlite3.Binary(cpickle.dumps(value))


class PWDB():
    def __init__(self, cfg, dirs, logger):
        self.logger = logger
        maindir = dirs["main"]
        self.sorted_nzbs_for_gui = None
        self.sorted_nzbshistory_for_gui = None
        self.cfg = cfg
        self.lock = threading.Lock()
        self.last_update_for_gui = 0
        self.db_file_name = maindir + "ginzibix.db"
        if os.path.isfile(self.db_file_name):
            self.db_file_exists = True
        else:
            self.db_file_exists = False

        try:
            self.db_file = CSqliteExtDatabase(self.db_file_name)
            self.db = CSqliteExtDatabase(":memory:")
        except Exception as e:
            self.logger.warning(whoami() + str(e))

        ipc_location = maindir + "ginzibix_socket1"
        self.wrapper_context = zmq.Context()
        self.wrapper_socket = self.wrapper_context.socket(zmq.REP)
        self.wrapper_socket.bind("ipc://" + ipc_location)
        # self.wrapper_socket.bind("tcp://*:37705")
        self.signal_ign_sigint = None
        self.signal_ign_sigterm = None

        class BaseModel(Model):
            class Meta:
                database = self.db

        class TimestampedModel(BaseModel):
            def save(self, *args, **kwargs):
                self.date_updated = datetime.datetime.now()
                return super(TimestampedModel, self).save(*args, **kwargs)

        class CONFIG(BaseModel):
            connection_health_threshold = IntegerField(default=65)

        class MSG(BaseModel):
            nzbname = CharField()
            timestamp = TimeField()
            message = CharField()
            level = CharField()

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
            verify_status = IntegerField(default=0)
            loadpar2vols = BooleanField(default=False)
            is_pw = BooleanField(default=False)
            password = CharField(default="N/A")
            bytes_in_resultqueue = IntegerField(default=0)
            resqlist_dill = DillField(default="N/A")
            allfilelist_dill = DillField(default="N/A")
            filetypecounter_dill = DillField(default="N/A")
            allfilesizes_dill = DillField(default="N/A")
            p2_dill = DillField(default="N/A")

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
            #   1 ... queued
            #   2 ... download success
            #   -1 .. download error
            # db_file_update_status(filename, 1)
            status = IntegerField(default=0)

        class ARTICLE(BaseModel):
            name = CharField()
            fileentry = ForeignKeyField(FILE, backref='articles')
            size = IntegerField(default=0)
            number = IntegerField(default=0)
            timestamp = TimeField()
            status = IntegerField(default=0)

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
        self.tablelist = [self.NZB, self.FILE, self.ARTICLE, self.MSG]

        if self.db_file_exists:
            try:
                self.db_file.backup(self.db)
                self.logger.debug(whoami() + "copied file db to :memory: db")
            except Exception as e:
                self.logger.warning(whoami())
        else:
            self.db.connect()
            self.db.create_tables(self.tablelist)

        self.SQLITE_MAX_VARIABLE_NUMBER = int(max_sql_variables() / 4)

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
        all_sorted_nzbs, _ = self.db_nzb_getall_sortedV3()
        for nzbdata in all_sorted_nzbs:
            n_name, n_prio, n_timestamp, n_status, n_size, n_dlsize = nzbdata
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

    # ---- self.MSG --------
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
        self.last_update_for_gui = time.time()
        if new_msg:
            return True
        else:
            return False

    def db_msg_get(self, nzbname0):
        msglist = []
        try:
            msg0 = self.MSG.select().where(self.MSG.nzbname == nzbname0).order_by(self.MSG.timestamp.desc())
            msglist = [(msg.message, msg.timestamp, msg.level) for msg in msg0]
            return msglist
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_msg_removeall(self, nzbname0):
        try:
            query = self.MSG.delete().where(self.MSG.nzbname == nzbname0)
            query.execute()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    # ---- self.NZB --------
    def db_nzb_are_all_nzb_idle(self):
        try:
            busy_nzb = self.NZB.select().where((self.NZB.status == 1) | (self.NZB.status == 2)
                                               | (self.NZB.status == 3)).order_by(self.NZB.priority)[0]
            if busy_nzb.name:
                return False
            else:
                return True
        except Exception as e:
            return True

    def db_nzb_getnextnzb_for_download(self):
        try:
            nzb = self.NZB.select().where((self.NZB.status == 1) | (self.NZB.status == 2)
                                          | (self.NZB.status == 3)).order_by(self.NZB.priority)[0]
        except Exception as e:
            return None
        return nzb.name

    def db_nzb_store_resqlist(self, nzbname, resqlist):
        try:
            query = self.NZB.update(resqlist_dill=resqlist, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_nzb_get_resqlist(self, nzbname):
        try:
            nzb0 = self.NZB.get(self.NZB.name == nzbname)
            resqlist = nzb0.resqlist_dill
            if resqlist == "N/A":
                return None
            return resqlist
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    def db_nzb_store_allfile_list(self, nzbname, allfilelist, filetypecounter, overall_size, overall_size_wparvol, p2):
        if not nzbname:
            return False
        try:
            query = self.NZB.update(allfilelist_dill=allfilelist, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
            query = self.NZB.update(filetypecounter_dill=filetypecounter, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
            already_downloaded_size = self.calc_already_downloaded_size(nzbname)
            allfilesizes0 = (overall_size, overall_size_wparvol, already_downloaded_size)
            query = self.NZB.update(allfilesizes_dill=allfilesizes0, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
            query = self.NZB.update(p2_dill=p2, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
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

    def db_nzb_insert(self, name0):
        try:
            prio = max([n.priority for n in self.NZB.select().order_by(self.NZB.priority)]) + 1
        except ValueError as e:
            prio = 1
        try:
            new_nzb = self.NZB.create(name=name0, priority=prio, timestamp=time.time())
            new_nzbname = new_nzb.name
        except Exception as e:
            new_nzbname = None
            self.logger.warning(whoami() + str(e))
        return new_nzbname

    def db_nzb_delete(self, nzbname):
        files = self.FILE.select().where(self.FILE.nzb.name == nzbname)
        for f0 in files:
            query_articles = self.ARTICLE.delete().where(self.ARTICLE.fileentry.orig_name == f0.orig_name)
            query_articles.execute()
        query_files = self.FILE.delete().where(self.FILE.nzb.name == nzbname)
        query_files.execute()
        query_nzb = self.NZB.delete().where(self.NZB.name == nzbname)
        query_nzb.execute()

    def db_nzb_loadpar2vols(self, name):
        try:
            nzb0 = self.NZB.get(self.NZB.name == name)
            return nzb0.loadpar2vols
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return False

    def db_nzb_set_bytes_in_resultqueue(self, nzbname, resqueue_size):
        query = self.NZB.update(bytes_in_resultqueue=resqueue_size, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
        query.execute()

    def db_nzb_update_loadpar2vols(self, name0, lp2):
        query = self.NZB.update(loadpar2vols=lp2, date_updated=datetime.datetime.now()).where(self.NZB.name == name0)
        query.execute()
        '''with self.db.atomic():
            try:
                nzb0 = self.NZB.get((self.NZB.name == name))
                nzb0.loadpar2vols = lp2
                nzb0.save()
            except Exception as e:
                self.logger.warning(str(e))'''

    def db_nzb_getsize(self, name):
        nzb0 = self.NZB.get(self.NZB.name == name)
        size = 0
        for a in nzb0.files:
            size += self.db_file_getsize(a.orig_name)
        return size

    def db_nzb_get_downloadedsize(self, name):
        try:
            nzb0 = self.NZB.get(self.NZB.name == name)
            size = 0
            for a in nzb0.files:
                size += self.db_file_get_downloadedsize(a.orig_name)
            return size
        except Exception as e:
            return 0

    def db_nzb_exists(self, name):
        try:
            nzb = self.NZB.get(self.NZB.name == name)
            assert(nzb.name)
            return True
        except Exception as e:
            return False

    def db_nzb_deleteall(self):
        query = self.NZB.delete()
        query.execute()

    def db_nzb_getall(self):
        nzbs = []
        for n in self.NZB.select():
            nzbs.append((n.name, n.priority, n.timestamp, n.status))
        return nzbs

    def db_nzb_getall_sortedV2(self):
        query = self.NZB.select().order_by(+self.NZB.priority)
        nzblist = [(n.name, n.priority, n.timestamp, n.status, self.db_nzb_getsize(n.name), n.bytes_in_resultqueue + self.db_nzb_get_downloadedsize(n.name))
                   for n in query if n.status in [1, 2, 3]]
        query = self.NZB.select().order_by(-self.NZB.date_updated)
        nzblist_history = [(n.name, n.priority, n.date_updated, n.status, self.db_nzb_getsize(n.name), n.bytes_in_resultqueue
                            + self.db_nzb_get_downloadedsize(n.priority)) for n in query if n.status in [4, -1, -2, -3, -4]]
        return nzblist, nzblist_history

    def db_nzb_getall_sortedV3(self):
        query = self.NZB.select().order_by(+self.NZB.priority)
        nzblist = [(n.name, n.priority, n.timestamp, n.status, self.db_nzb_getsize(n.name), n.bytes_in_resultqueue + self.db_nzb_get_downloadedsize(n.name))
                   for n in query]
        query = self.NZB.select().order_by(-self.NZB.date_updated)
        nzblist_history = [(n.name, n.priority, n.date_updated, n.status, self.db_nzb_getsize(n.name), n.bytes_in_resultqueue
                            + self.db_nzb_get_downloadedsize(n.priority)) for n in query if n.status in [4, -1, -2, -3, -4]]
        return nzblist, nzblist_history

    def db_nzb_getall_sorted(self):
        query = self.NZB.select()
        nzbs = []
        for n in query:
            # only return nzbs with valid status
            if n.status in [1, 2, 3]:
                resqueue_size = n.bytes_in_resultqueue
                nzbs.append((n.name, n.priority, n.timestamp, n.status, self.db_nzb_getsize(n.name), resqueue_size + self.db_nzb_get_downloadedsize(n.name)))
        if nzbs:
            return sorted(nzbs, key=lambda nzb: nzb[1])
        else:
            return []

    def db_nzb_set_password(self, nzbname, pw):
        query = self.NZB.update(password=pw, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
        query.execute()

    def db_nzb_get_password(self, nzbname):
        try:
            query = self.NZB.get(self.NZB.name == nzbname)
            return query.password
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_nzb_set_ispw(self, nzbname, ispw):
        query = self.NZB.update(is_pw=ispw, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
        query.execute()

    def db_nzb_get_ispw(self, nzbname):
        try:
            query = self.NZB.get(self.NZB.name == nzbname)
            return query.is_pw
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_nzb_update_unrar_status(self, nzbname, newstatus):
        query = self.NZB.update(unrar_status=newstatus, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
        query.execute()

    def db_nzb_update_verify_status(self, nzbname, newstatus):
        query = self.NZB.update(verify_status=newstatus, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
        query.execute()

    def db_nzb_update_status(self, nzbname, newstatus):
        try:
            query = self.NZB.update(status=newstatus, date_updated=datetime.datetime.now()).where(self.NZB.name == nzbname)
            query.execute()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_nzb_getstatus(self, nzbname):
        try:
            query = self.NZB.get(self.NZB.name == nzbname)
            return query.status
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_nzb_get_unrarstatus(self, nzbname):
        try:
            query = self.NZB.get(self.NZB.name == nzbname)
            return query.unrar_status
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_nzb_get_verifystatus(self, nzbname):
        try:
            query = self.NZB.get(self.NZB.name == nzbname)
            return query.verify_status
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

    def db_file_getftype_renamed(self, name):
        try:
            file0 = self.FILE.get(self.FILE.renamed_name == name)
            return file0.ftype
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_getsize(self, name):
        file0 = self.FILE.get(self.FILE.orig_name == name)
        size = 0
        for a in file0.articles:
            size += a.size
        return size

    def db_file_get_downloadedsize(self, name):
        file0 = self.FILE.get(self.FILE.orig_name == name)
        if file0.status != 2:
            return 0
        return sum([a.size for a in file0.articles])

    def db_file_getsize_renamed(self, name):
        file0 = self.FILE.get(self.FILE.renamed_name == name)
        size = 0
        for a in file0.articles:
            size += a.size
        return size

    def db_allnonrarfiles_getstate(self, nzbname):
        files00 = self.FILE.select()
        files0 = [f0 for f0 in files00 if f0.nzb.name == nzbname]
        statusok = True
        for f0 in files0:
            if f0.ftype not in ["rar", "par2", "par2vol"] and f0.status <= 0:
                self.logger.info("!!!! " + f0.orig_name + " / " + str(f0.status))
                statusok = False
                # break
        return statusok

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
            rarfiles = [rf for rf in self.NZB.get(self.NZB.name == nzbname).files if rf.ftype == "rar"]
            rarl = [(r.parverify_state, r.orig_name) for r in rarfiles]
            rarstates = [r.parverify_state for r in rarfiles]
            if (0 not in rarstates):
                return True, rarl
            return False, rarl
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None, rarl

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
            # par2 = self.FILE.get(self.FILE.ftype == "par2", self.FILE.renamed_name != "N/A", self.FILE.nzb.name == nzbname)
            return par2.renamed_name
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None

    def db_file_update_status(self, filename, newstatus):
        query = self.FILE.update(status=newstatus).where(self.FILE.orig_name == filename)
        query.execute()
        file0 = self.FILE.get((self.FILE.orig_name == filename))
        file0.nzb.date_updated = datetime.datetime.now()
        file0.nzb.save()

    def db_file_update_parstatus(self, filename, newparstatus):
        query = self.FILE.update(parverify_state=newparstatus).where(self.FILE.orig_name == filename)
        query.execute()

    def db_file_update_nzbstatus(self, filename0, newnzbstatus0):
        file0 = self.FILE.get((self.FILE.orig_name == filename0))
        file0.nzb.status = newnzbstatus0
        file0.save()

    def db_file_set_renamed_name(self, orig_name0, renamed_name0):
        try:
            query = self.FILE.update(renamed_name=renamed_name0).where(self.FILE.orig_name == orig_name0)
            query.execute()
        except Exception as e:
            self.logger.warning(whoami() + str(e))

    def db_file_set_file_type(self, orig_name0, ftype0):
        query = self.FILE.update(ftype=ftype0).where(self.FILE.orig_name == orig_name0)
        query.execute()

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

    def db_file_insert(self, name, nzbname, nr_articles, age, ftype):
        try:
            nzb0 = self.db_nzbname_to_nzbentry(nzbname)
            self.FILE.create(orig_name=name, nzb=nzb0, nr_articles=nr_articles, age=age, ftype=ftype, timestamp=time.time())
            new_file = name
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

    def db_file_deleteall(self):
        query = self.FILE.delete()
        query.execute()

    # ---- self.ARTICLE --------
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

    def db_article_deleteall(self):
        query = self.ARTICLE.delete()
        query.execute()

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
            except OperationalError as e:
                chunksize = int(chunksize * 0.9)
                continue
            except Exception as e:
                self.logger.warning(whoami() + str(e))
            i += chunksize
        self.SQLITE_MAX_VARIABLE_NUMBER = chunksize

    # ---- self.DB --------
    def db_close(self):
        self.db_file.close()
        self.db.close()

    def db_drop(self):
        self.db.drop_tables(self.tablelist)

    # ---- set new prios acc. to nzb list ----
    def set_nzbs_prios(self, new_nzb_list, delete=False):
        oldnzb_0 = self.NZB.select().order_by(self.NZB.priority)[0]

        if not new_nzb_list:
            return True, oldnzb_0.name

        first_has_changed = True
        if oldnzb_0.name == new_nzb_list[0]:
            first_has_changed = False

        old_nzb_list = []
        for n in self.NZB.select().order_by(self.NZB.priority):
            old_nzb_list.append(n.name)

        del_nzb_name = None
        if delete:
            del_nzb_name = None
            del_nzb_index = -1
            for i, onzb in enumerate(old_nzb_list):
                if onzb not in new_nzb_list:
                    del_nzb_name = onzb
                    del_nzb_index = i
                    break
            if del_nzb_name:
                del old_nzb_list[del_nzb_index]

        mi_overflow = 1
        for i, onzb in enumerate(old_nzb_list):
            try:
                matchidx = [j for j, name in enumerate(new_nzb_list) if name == onzb][0]
            except Exception as e:
                matchidx = len(new_nzb_list) + mi_overflow
                mi_overflow += 1
            query = self.NZB.update(priority=matchidx+1, date_updated=datetime.datetime.now()).where(self.NZB.name == onzb)
            query.execute()
        return first_has_changed, del_nzb_name

    # ---- send sorted nzbs to guiconnector ---
    def get_stored_sorted_nzbs(self):
        return self.sorted_nzbs_for_gui, self.sorted_nzbshistory_for_gui

    def store_sorted_nzbs(self):
        sortednzbs, sortednzbs_history = self.db_nzb_getall_sortedV2()
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
        for fname0 in glob.glob(dir0 + "*"):
            short_fn = fname0.split("/")[-1]
            if short_fn == file0.orig_name or short_fn == file0.renamed_name:
                file_already_exists = True
                break
        return dir0 + file0.orig_name, file_already_exists

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
        ad_size = 0
        gbdivisor = (1024 * 1024 * 1024)
        try:
            nzb = self.NZB.get(self.NZB.name == nzbname)
            files = [files0 for files0 in nzb.files]
        except Exception as e:
            self.logger.debug(whoami() + str(e))
            return 0
        for f0 in files:
            if f0.status in [2, -1]:
                articles = [articles0 for articles0 in f0.articles]
                f0size = sum([a.size for a in articles])
                ad_size += f0size
        # print("Size already_downloaded_size: ", ad_size / gbdivisor)
        resqlist = self.db_nzb_get_resqlist(nzbname)
        if not resqlist:
            return ad_size / gbdivisor
        for _, _, _, _, _, art_name, _, inf0, _ in resqlist:
            art_size = 0
            if inf0 != "failed":
                art_size = sum(len(i) for i in inf0)
                ad_size += art_size
        # print("Size already_downloaded_size + resqlist: ", ad_size / gbdivisor)
        return ad_size / gbdivisor

    def create_allfile_list_via_name(self, nzbname, dir0):
        try:
            nzb = self.NZB.get(self.NZB.name == nzbname)
            return self.create_allfile_list(nzb, dir0)
        except Exception as e:
            self.logger.warning(whoami() + str(e))
            return None, None

    def create_allfile_list(self, nzb, dir0):
        nzbname = nzb.name
        resqlist = None
        gbdivisor = (1024 * 1024 * 1024)
        if nzb.status == 2:
            resqlist = self.db_nzb_get_resqlist(nzbname)
        res = self.db_nzb_get_allfile_list(nzbname)
        if res:
            allfilelist, filetypecounter, overall_size, overall_size_wparvol, _, p2 = res
            return filetypecounter, nzbname
        else:
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
            resqlist_size = 0
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
                        if resqlist:
                            art_found = False
                            for fn_r, age_r, ft_r, nr_art_r, art_nr_r, art_name_r, download_server_r, inf0_r, add_bytes in resqlist:
                                if a.name == art_name_r:
                                    art_found = True
                                    break
                            if art_found:
                                if inf0_r != "failed":
                                    asize0 = sum(len(i) for i in inf0_r)
                                    resqlist_size += asize0
                                    already_downloaded_size += asize0
                idx += 1
            if allfilelist:
                self.db_nzb_update_status(nzbname, 1)
                overall_size /= gbdivisor
                overall_size_wparvol /= gbdivisor
                overall_size_wparvol += overall_size
                already_downloaded_size /= gbdivisor
                # save results in db
                self.db_nzb_store_allfile_list(nzbname, allfilelist, filetypecounter, overall_size, overall_size_wparvol, p2)
                return filetypecounter, nzbname
            else:
                self.db_nzb_update_status(nzbname, 2)
                return None, None

    # ---- make_allfilelist -------
    #      makes a file/articles list out of top-prio nzb, ready for beeing queued
    #      to download threads
    def make_allfilelist(self, dir0, nzbdir):
        try:
            nzb = self.NZB.select().where((self.NZB.status == 1) | (self.NZB.status == 2)
                                          | (self.NZB.status == 3)).order_by(self.NZB.priority)[0]

        except Exception as e:
            return None
        self.logger.info(whoami() + "analyzing NZB: " + nzb.name + " with status: " + str(nzb.status))
        nzbname = nzb.name
        nzbstatus = self.db_nzb_getstatus(nzbname)
        nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
        incompletedir = dir0 + nzbdir
        # state "queued"
        if nzbstatus == 1:
            files = [files0 for files0 in nzb.files]   # if files0.status in [0, 1]]
            if not files:
                self.logger.info(whoami() + "No files to download for NZB " + nzb.name)
                self.db_nzb_update_status(nzbname, -1)     # download failed as no files present
                return None
            # if queued and incomplete dir exists -> delete, because of inconsistent state!
            try:
                if os.path.isdir(incompletedir):
                    self.logger.info(whoami() + "Removing incomplete_dir")
                    shutil.rmtree(incompletedir)
            except Exception as e:
                self.logger.error(whoami() + str(e) + ": error in removing incomplete_dir")
                self.db_nzb_update_status(nzbname, -1)     # download failed as no files present
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
            rars_in_verified_dir = [fname0.split("/")[-1] for fname0 in glob.glob(dir_verified + "*")]
            if not lists_are_equal(verified_files, rars_in_verified_dir):
                self.logger.warning(whoami() + "inconsistency in verified rars, deleting nzb in db")
                self.nzb_reset(nzbname, incompletedir, nzbdir)
                return None
            # check consistency: renamed
            self.logger.debug(whoami() + "verified_dir consistent, checking renamed_dir")
            renamed_files = [f0.renamed_name for f0 in files if f0.renamed_name != "N/A"]
            files_in_renamed_dir = [fname0.split("/")[-1] for fname0 in glob.glob(dir_renamed + "*")]
            files_in_renamed_dir = [f0 for f0 in files_in_renamed_dir if f0[-6:-2] != ".rar."]
            if not lists_are_equal(renamed_files, files_in_renamed_dir):
                self.logger.warning(whoami() + "inconsistency in renamed_dir, deleting nzb in db/filesystem")
                self.nzb_reset(nzbname, incompletedir, nzbdir)
                return None
            # check if all downloaded files exist in _renamed or _downloaded0
            self.logger.debug(whoami() + "renamed_dir consistent, checking if all downloaded files exist")
            files_in_downloaded_dir = [fname0.split("/")[-1] for fname0 in glob.glob(dir_downloaded + "*")]
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


def wrapper_main(cfg, dirs, logger):
    logger.debug(whoami() + "starting ...")

    pwwt = PWDB(cfg, dirs, logger)
    pwwt.signal_ign_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pwwt.signal_ign_sigterm = signal.signal(signal.SIGTERM, signal.SIG_IGN)

    pwwt.do_loop()

    try:
        pwwt.db.backup(pwwt.db_file)
        pwwt.db_drop()
        pwwt.db_close()
    except Exception as e:
        logger.warning(whoami() + str(e))

    logger.debug(whoami() + "exited!")


'''if __name__ == "__main__":

    import logging
    import logging.handlers

    logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    pwdb = PWDB(logger)

    nzbname = 'Ubuntu16.04.nzb'
    aok = pwdb.db_get_all_ok_nonrarfiles(nzbname)
    stat = pwdb.db_nzb_getstatus(nzbname)

    print(aok)
    print("orig_name, renamed_name, ftype, nzb.name, timestamp, status, parverify_state, f.age, f.nr_articles, f.status")
    fall = pwdb.db_file_getall()
    for f in fall:
        print(f)

    pp2 = pwdb.get_renamed_p2("/home/stephan/.ginzibix/incomplete/Ubuntu16.04/_renamed0/", nzbname)
    print(pp2)'''
