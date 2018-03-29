from peewee import SqliteDatabase, Model, CharField, ForeignKeyField, TextField, DateTimeField, BooleanField, IntegerField, IntegrityError, TimeField
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


# ---- NZB ----------------------------------------------------------------------


class NZB(BaseModel):
    name = CharField(unique=True)
    priority = IntegerField(default=-1)
    size = IntegerField(default=0)
    timestamp = TimeField(default=time.time())
    status = IntegerField(default=0)


def db_nzb_insert(name, size):
    try:
        prio = max([n.priority for n in NZB.select().order_by(NZB.priority)]) + 1
    except ValueError:
        prio = 1
    try:
        new_nzb = NZB.create(name=name, priority=prio, size=size)
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


def db_nzb_update_size(name, size):
    nzb = NZB.get(NZB.name == name)
    if nzb:
        nzb.size = size
        nzb.save()
    return nzb


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
        nzbs.append((n.name, n.priority, n.size, n.timestamp, n.status))
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


# ---- DB ----------------------------------------------------------------------

def db_close():
    DB.close()


def db_drop():
    DB.drop_tables(TABLE_LIST)

# ---- MAIN --------------------------------------------------------------------


TABLE_LIST = [NZB, FILE, ARTICLE]
DB.connect()
DB.create_tables(TABLE_LIST)

# new_nzb = db_nzb_insert("abc.nzb", 100)
# new_nzb1 = db_nzb_insert("abc2.nzb", 100)

# print(db_nzb_getall())

# db_nzb_deleteall()
