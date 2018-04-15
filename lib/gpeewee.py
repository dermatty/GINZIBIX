from peewee import SqliteDatabase, Model, CharField, ForeignKeyField, IntegerField, IntegrityError, TimeField, OperationalError
import os
from os.path import expanduser
import time
from .par2lib import calc_file_md5hash
import glob
import re

'''file status:
    0 .... not queued yet
    1 ... downloading
    2 ... done, ok
    -1 ... done, errors'''

lpref = __name__ + " - "


class PWDB:
    def __init__(self, logger):
        maindir = os.environ.get("GINZIBIX_MAINDIR")
        if not maindir:
            userhome = expanduser("~")
            maindir = userhome + "/.ginzibix/"
        if maindir[-1] != "/":
            maindir += "/"
        self.db = SqliteDatabase(maindir + "ginzibix.db")
        self.logger = logger

        class BaseModel(Model):
            class Meta:
                database = self.db

        class NZB(BaseModel):
            name = CharField(unique=True)
            priority = IntegerField(default=-1)
            timestamp = TimeField()
            status = IntegerField(default=0)

        class FILE(BaseModel):
            orig_name = CharField()
            renamed_name = CharField(default="N/A")
            parverify_state = IntegerField(default=0)
            unrared_state = IntegerField(default=0)
            nzb = ForeignKeyField(NZB, backref='files')
            nr_articles = IntegerField(default=0)
            age = IntegerField(default=0)
            ftype = CharField()
            timestamp = TimeField()
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

        self.NZB = NZB
        self.FILE = FILE
        self.ARTICLE = ARTICLE
        self.tablelist = [self.NZB, self.FILE, self.ARTICLE]
        self.db.connect()
        self.db.create_tables(self.tablelist)
        self.SQLITE_MAX_VARIABLE_NUMBER = int(max_sql_variables() / 4)

    # ---- self.NZB --------
    def db_nzb_insert(self, name0):
        try:
            prio = max([n.priority for n in self.NZB.select().order_by(self.NZB.priority)]) + 1
        except ValueError:
            prio = 1
        try:
            new_nzb = self.NZB.create(name=name0, priority=prio, timestamp=time.time())
        except Exception as e:
            new_nzb = None
            self.logger.warning(lpref + name0 + ": " + str(e))
        return new_nzb

    def db_nzb_getsize(self, name):
        nzb0 = self.NZB.get(self.NZB.name == name)
        size = 0
        for a in nzb0.files:
            size += self.db_file_getsize(a.orig_name)
        return size

    def db_nzb_exists(self, name):
        try:
            nzb = self.NZB.get(self.NZB.name == name)
            return nzb
        except Exception as e:
            return None

    def db_nzb_deleteall(self):
        query = self.NZB.delete()
        query.execute()

    def db_nzb_getall(self):
        nzbs = []
        for n in self.NZB.select():
            nzbs.append((n.name, n.priority, n.timestamp, n.status))
        return nzbs

    def db_nzb_update_status(self, nzbname, newstatus):
        nzb0 = self.NZB.get((self.NZB.name == nzbname))
        nzb0.status = newstatus
        nzb0.save()

    def db_nzb_getstatus(self, nzbname):
        try:
            query = self.NZB.get(self.NZB.name == nzbname)
            return query.status
        except:
            return None

    # ---- self.FILE --------
    def db_file_getsize(self, name):
        file0 = self.FILE.get(self.FILE.orig_name == name)
        size = 0
        for a in file0.articles:
            size += a.size
        return size

    def db_file_update_status(self, filename, newstatus):
        file0 = self.FILE.get((self.FILE.orig_name == filename))
        file0.status = newstatus
        file0.save()
        # query = self.FILE.update(status=newstatus).where(self.FILE.orig_name == filename)
        # query.execute()

    def db_file_getstatus(self, filename):
        try:
            query = self.FILE.get(self.FILE.orig_name == filename)
            return query.status
        except:
            return None

    def db_file_insert(self, name, nzb, nr_articles, age, ftype):
        try:
            new_file = self.FILE.create(orig_name=name, nzb=nzb, nr_articles=nr_articles, age=age, ftype=ftype, timestamp=time.time())
        except Exception as e:
            new_file = None
            self.logger.warning(lpref + str(e))
        return new_file

    def db_file_getall(self):
        files = []
        for f in self.FILE.select():
            files.append((f.orig_name, f.renamed_name, f.nzb, f.timestamp, f.status, f.parverify_state,
                          f.unrared_state, f.age, f.nr_articles))
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
            try:
                with self.db.atomic():
                    query = self.ARTICLE.insert_many(data0, fields=[self.ARTICLE.name, self.ARTICLE.fileentry, self.ARTICLE.size, self.ARTICLE.number,
                                                                    self.ARTICLE.timestamp])
                    query.execute()
            except OperationalError:
                chunksize = int(chunksize * 0.9)
                continue
            i += chunksize
        self.SQLITE_MAX_VARIABLE_NUMBER = chunksize

    # ---- self.DB --------
    def db_close(self):
        self.db.close()

    def db_drop(self):
        self.db.drop_tables(self.tablelist)

    # ---- get_downloaded_file_full_path ----
    def get_downloaded_file_full_path(self, file0, dir0):
        file_already_exists = False
        # self.logger.info(lpref + dir0 + "*")
        for fname0 in glob.glob(dir0 + "*"):
            short_fn = fname0.split("/")[-1]
            if short_fn == file0.orig_name:
                file_already_exists = True
                break
        return dir0 + file0.orig_name, file_already_exists

    # ---- make_allfilelist -------
    #      makes a file/articles list out of top-prio nzb, ready for beeing queued
    #      to download threads
    def make_allfilelist(self, dir0):
        allfilelist = []
        filetypecounter = {"rar": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "nfo": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "par2": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "par2vol": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "sfv": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []},
                           "etc": {"counter": 0, "max": 0, "filelist": [], "loadedfiles": []}}
        try:
            nzb = self.NZB.select().where((self.NZB.status == 0) | (self.NZB.status == 1)).order_by(self.NZB.priority)[0]
        except Exception as e:
            self.logger.info(lpref + str(e) + ": no NZBs to queue")
            return None, None, None
        nzbname = nzb.name
        self.logger.info(lpref + nzbname + ", status: " + str(self.db_nzb_getstatus(nzbname)))
        nzbdir = re.sub(r"[.]nzb$", "", nzbname, flags=re.IGNORECASE) + "/"
        dir00 = dir0 + nzbdir + "_downloaded0/"
        files = [files0 for files0 in nzb.files]   # if files0.status in [0, 1]]
        if not files:
            self.logger.info(lpref + "No files to download for NZB " + nzb.name)
            return None, None, None
        idx = 0
        for f0 in files:
            filetypecounter[f0.ftype]["max"] += 1
            filetypecounter[f0.ftype]["filelist"].append(f0.orig_name)
            self.logger.info(lpref + f0.orig_name + ", status in db: " + str(f0.status))
            if f0.status not in [0, 1]:
                # todo:
                #    calc md5 hash
                #    filename0 is real path of file
                filename0, file_already_exists = self.get_downloaded_file_full_path(f0, dir00)
                self.logger.info(lpref + filename0 + ", found on dir: " + str(file_already_exists))
                if file_already_exists:
                    filetypecounter[f0.ftype]["counter"] += 1
                    md5 = calc_file_md5hash(filename0)
                    filetypecounter[f0.ftype]["loadedfiles"].append((f0.orig_name, filename0, md5))
                    continue
                else:
                    self.db_file_update_status(f0.orig_name, 0)
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
            self.db_nzb_update_status(nzbname, 1)
            return allfilelist, filetypecounter, nzbname
        else:
            self.db_nzb_update_status(nzbname, 2)
            return None, None, None


''''
# ---- QUEUER -----------------------------------------------------------------

def db_queue_files(max_cache):
    # get articles right now processed
    articlesrunning = [article for article in ARTICLE.select().where(ARTICLE.status == 0)]
    artrun_size = sum([article.size for article in articlesrunning])
    if artrun_size >= max_cache:
        return None
    # get not finalized nzb
    nzbs = [nzb for nzb in NZB.select().where(NZB.status in [0, 1]).order_by(NZB.priority)]
    if not nzbs:
        return None
    # now, get all not yet downloaded files for these NZBs
    files = [files0 for files0 in nzbs.files if files0.status in [0, 1]]
    if not files:
        return None
    # finally, all not downloaded articles
    articles = [articles0 for articles0 in files.articles if articles0.status == 0]
    if not articles:
        return None
    newarticles = []
    size0 = artrun_size
    i = 0
    while size0 < max_cache and i < len(articles):
        newarticles.append((articles[i].name, articles[i].fileentry.orig_name, articles[i].fileentry.nzb.name))
        size0 += articles[i].size
        i += 1
    # todo:
    #       check if nzbpriority is preserved
    #       move par2 files(articles that belong to par2files) to the beginning of newarticles
    #                  i.e. oben files = [files0 ] .... par2s vorziehen
return newarticles'''
