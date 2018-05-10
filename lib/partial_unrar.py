import re
import inotify_simple
import glob
import os
import pexpect
import time

lpref = __name__ + " - "


def get_inotify_events(inotify):
    rar_events = []
    for event in inotify.read():
        str0 = event.name
        is_created_file = False
        flgs0 = []
        for flg in inotify_simple.flags.from_mask(event.mask):
            if "flags.CREATE" in str(flg) and "flags.ISDIR" not in str(flg):
                flgs0.append(str(flg))
                is_created_file = True
        if not is_created_file:
            continue
        gg_rar = re.search(r"\S*[.]rar", str0, flags=re.IGNORECASE)
        if gg_rar:
            rar_events.append((gg_rar.group(), flgs0))
    return rar_events


def get_rar_files(directory):
    rarlist = []
    for rarf in glob.glob("*.rar"):
        gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
        rarlist.append((int(gg.group().split(".")[0]), rarf))
    return rarlist


def partial_unrar(directory, unpack_dir, pwdb, nzbname, logger, password):
    # logger.debug(lpref + "dir: " + directory + " / unpack: " + unpack_dir)
    cwd0 = os.getcwd()
    try:
        os.chdir(directory)
    except FileNotFoundError:
        os.mkdir(directory)

    # get already present rar files
    while True:
        rar_basislist = get_rar_files(directory)
        rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])
        if rar_sortedlist and rar_sortedlist[0][0] == 1:
            logger.debug(lpref + "1st rar appeared: " + str(rar_sortedlist))
            break
        time.sleep(1)

    pwdb.db_nzb_update_unrar_status(nzbname, 1)
    # first valid rar_sortedlist in place, start unrar!
    pwdstr = ""
    if password:
        pwdstr = "-p" + password
    logger.debug(lpref + "executing 'unrar x -y -o+ -vp " + pwdstr + " '" + rar_sortedlist[0][1] + "' '" + unpack_dir + "'")
    cmd = "unrar x -y -o+ -vp " + pwdstr + " '" + directory + rar_sortedlist[0][1] + "' '" + unpack_dir + "'"
    child = pexpect.spawn(cmd)
    status = 1      # 1 ... running, 0 ... exited ok, -1 ... rar corrupt, -2 ..missing rar, -3 ... unknown error
    while True:
        str0 = ""
        while True:
            try:
                a = child.read_nonblocking().decode("utf-8")
                str0 += a
            except pexpect.exceptions.EOF:
                break
            if str0[-6:] == "[Q]uit":
                break
        # logger.debug(lpref + str0)
        gg = re.search(r"\S*[.]rar", str0, flags=re.IGNORECASE)
        if "error" in str0:
            if "packed data checksum" in str0:
                statmsg = "packed data checksum error (= corrupt rar!)"
                status = -1
            elif "- checksum error" in str0:
                statmsg = "checksum error (= rar is missing!)"
                status = -2
            else:
                statmsg = "unknown error"
                status = -3
            logger.info(lpref + gg.group() + statmsg)
            break
        else:
            logger.info(lpref + gg.group() + ": unrar success!")
        if "All OK" in str0:
            statmsg = "All OK"
            status = 0
            break
        try:
            gg = re.search(r"Insert disk with ", str0, flags=re.IGNORECASE)
            gend = gg.span()[1]
            nextrarname = str0[gend:-19]
            # gg = re.search(r"\S*[.]rar\s[[]C", str0, flags=re.IGNORECASE)
            # nextrarname = gg.group()[:-3]
        except Exception as e:
            logger.warning(lpref + str(e) + ", unknown error")
            statmsg = "unknown error in re evalution"
            status = -4
            break
        logger.debug(lpref + "Waiting for next rar: " + nextrarname)
        gotnextrar = False
        while not gotnextrar:
            time.sleep(1)
            for f0 in glob.glob(directory + "*.rar"):
                # f0 = f0.split("/")[-1]
                if nextrarname == f0:
                    try:
                        gotnextrar = True
                        break
                    except Exception as e:
                        logger.warning(lpref + str(e))
        time.sleep(1)   # achtung hack!
        child.sendline("C")
    logger.info(lpref + str(status) + " " + statmsg)
    os.chdir(cwd0)
    if status == 0:
        pwdb.db_nzb_update_unrar_status(nzbname, 2)
    else:
        pwdb.db_nzb_update_unrar_status(nzbname, -1)
