from peewee import SqliteDatabase, Model, CharField, ForeignKeyField, TextField, DateTimeField, BooleanField, IntegerField, IntegrityError, TimeField, OperationalError
import os
from os.path import expanduser
import time
import logging

logger = logging.getLogger(__name__)

maindir = os.environ.get("GINZIBIX_MAINDIR")
if not maindir:
    userhome = expanduser("~")
    maindir = userhome + "/.ginzibix/"
if maindir[-1] != "/":
    maindir += "/"


DB = SqliteDatabase(maindir + "ginzibix.db")


class BaseModel(Model):
    class Meta:
        database = DB

# status in NZB / FILE / ARTICLE
#     0 ... not queued yet
#     1 ... downloading (not all files/articles queued yet)
#     2 ... downloading (all queued)
#     3 ... done, ok
#     -1 ... done, failed


# ---- NZB ----------------------------------------------------------------------


class NZB(BaseModel):
    name = CharField(unique=True)
    priority = IntegerField(default=-1)
    timestamp = TimeField(default=time.time())
    status = IntegerField(default=0)


def db_nzb_insert(name):
    try:
        prio = max([n.priority for n in NZB.select().order_by(NZB.priority)]) + 1
    except ValueError:
        prio = 1
    try:
        new_nzb = NZB.create(name=name, priority=prio)
    except Exception as e:
        new_nzb = None
        logger.warning(name + ": " + str(e))
    return new_nzb


def db_nzb_changeprio(name, increment=-1):
    incr = int(increment)
    if incr == 0:
        return
    nzb = NZB.get(NZB.name == name)
    newprio = nzb.priority + incr
    if increment > 0:
        for n in NZB.select().where(NZB.priority <= newprio):
            n.priority -= incr
            n.save()
    elif increment < 0:
        for n in NZB.select().where(NZB.priority >= newprio):
            n.priority += incr
            n.save()


def db_nzb_getsize(name):
    nzb0 = NZB.get(NZB.name == name)
    size = 0
    for a in nzb0.files:
        size += db_file_getsize(a.orig_name)
    return size


def db_nzb_exists(name):
    try:
        nzb = NZB.get(NZB.name == name)
        return nzb
    except:
        return None


def db_nzb_deleteall():
    query = NZB.delete()
    query.execute()


def db_nzb_getall():
    nzbs = []
    for n in NZB.select():
        nzbs.append((n.name, n.priority, n.timestamp, n.status))
    return nzbs


# ---- FILE --------------------------------------------------------------------

class FILE(BaseModel):
    orig_name = CharField()
    renamed_name = CharField(default="N/A")
    parverify_state = IntegerField(default=0)
    unrared_state = IntegerField(default=0)
    nzb = ForeignKeyField(NZB, backref='files')
    nr_articles = IntegerField(default=0)
    age = IntegerField(default=0)
    timestamp = TimeField(default=time.time())
    status = IntegerField(default=0)


def db_file_getsize(name):
    file0 = FILE.get(FILE.orig_name == name)
    size = 0
    for a in file0.articles:
        size += a.size
    return size


def db_file_insert(name, nzb, nr_articles, age):
    try:
        new_file = FILE.create(orig_name=name, nzb=nzb, nr_articles=nr_articles, age=age)
    except Exception as e:
        new_file = None
        logger.warning(str(e))
    return new_file


def db_file_getall():
    files = []
    for f in FILE.select():
        files.append((f.orig_name, f.renamed_name, f.nzb, f.timestamp, f.status, f.parverify_state,
                      f.unrared_state, f.age, f.nr_articles))
    return files


def db_file_deleteall():
    query = FILE.delete()
    query.execute()


# ---- ARTICLE -----------------------------------------------------------------

class ARTICLE(BaseModel):
    name = CharField()
    fileentry = ForeignKeyField(FILE, backref='articles')
    size = IntegerField(default=0)
    number = IntegerField(default=0)
    timestamp = TimeField(default=time.time())
    status = IntegerField(default=0)


def db_article_insert(name, fileentry, size, number):
    try:
        new_article = ARTICLE.create(name=name, fileentry=fileentry)
    except Exception as e:
        new_article = None
        logger.article(str(e))
    return new_article


def db_article_getall():
    articles = []
    for a in ARTICLE.select():
        articles.append((a.name, a.fileentry, a.timestamp, a.status))
    return articles


def db_article_deleteall():
    query = ARTICLE.delete()
    query.execute()


def db_article_insert_many(data):
    global SQLITE_MAX_VARIABLE_NUMBER
    i = 0
    chunksize = SQLITE_MAX_VARIABLE_NUMBER
    llen = len(data)
    while i < llen:
        data0 = data[i: min(i + chunksize, llen)]
        try:
            with DB.atomic():
                query = ARTICLE.insert_many(data0, fields=[ARTICLE.name, ARTICLE.fileentry, ARTICLE.size, ARTICLE.number])
                query.execute()
        except OperationalError:
            chunksize = int(chunksize * 0.9)
            continue
        i += chunksize
    SQLITE_MAX_VARIABLE_NUMBER = chunksize


def max_sql_variables():
    """Get the maximum number of arguments allowed in a query by the current
    sqlite3 implementation. Based on `this question
    `_

    Returns
    -------
    int
        inferred SQLITE_MAX_VARIABLE_NUMBER
    """

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
    return newarticles


# ---- DB ----------------------------------------------------------------------

def db_close():
    DB.close()


def db_drop():
    DB.drop_tables(TABLE_LIST)

# ---- MAIN --------------------------------------------------------------------


TABLE_LIST = [NZB, FILE, ARTICLE]
DB.connect()
DB.create_tables(TABLE_LIST)
SQLITE_MAX_VARIABLE_NUMBER = int(max_sql_variables() / 4)
