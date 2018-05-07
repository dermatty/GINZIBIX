import re
import inotify_simple
import glob
import os
import pexpect

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


def partial_unrar(directory, unpack_dir, pwdb, nzbname, logger):
    cwd0 = os.getcwd()
    try:
        os.chdir(directory)
    except FileNotFoundError:
        os.mkdir(directory)
    # init inotify
    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(directory, watch_flags)

    # get already present rar files
    eventslist = []
    rar_basislist = get_rar_files(directory)
    rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])

    # wait for first file to arrive before starting unrar if no rar files present
    while not rar_sortedlist or rar_sortedlist[0][0] != 1:
        events = get_inotify_events(inotify)
        if events not in eventslist:
            eventslist.append(events)
            rar_basislist = get_rar_files(directory)
            rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])

    pwdb.db_nzb_update_unrar_status(nzbname, 1)
    # first valid rar_sortedlist in place, start unrar!
    logger.info(lpref + "executing 'unrar x -y -o+ -vp'")
    cmd = "unrar x -y -o+ -vp " + rar_sortedlist[0][1] + " " + unpack_dir
    child = pexpect.spawn(cmd)
    status = 1      # 1 ... running, 0 ... exited ok, -1 ... rar corrupt, -2 ..missing rar, -3 ... unknown error
    rarindex = 1
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
        if "error" in str0:
            gg = re.search(r"\S*[.]rar", str0, flags=re.IGNORECASE)
            if "packed data checksum" in str0:
                logger.info(lpref + gg.group() + " : packed data checksum error (= corrupt rar!)")
                status = -1
            elif "- checksum error" in str0:
                logger.info(lpref + gg.group() + " : checksum error (= rar is missing!)")
                status = -2
            else:
                logger.info(lpref + gg.group() + " : unknown error")
                status = -3
            break
        if "All OK" in str0:
            statmsg = "All OK"
            status = 0
            break
        rarindex += 1
        if rarindex not in [nr for nr, _ in rar_sortedlist]:
            gotnextrar = False
            while not gotnextrar:
                events = get_inotify_events(inotify)
                if events not in eventslist:
                    eventslist.append(events)
                    rar_basislist = get_rar_files(directory)
                    rar_sortedlist = sorted(rar_basislist, key=lambda nr: nr[0])
                    if rarindex in [nr for nr, _ in rar_sortedlist]:
                        gotnextrar = True
        child.sendline("C")
    logger.info(lpref + str(status) + " " + statmsg)
    os.chdir(cwd0)
    if status == 0:
        pwdb.db_nzb_update_unrar_status(nzbname, 2)
    else:
        pwdb.db_nzb_update_unrar_status(nzbname, -1)
