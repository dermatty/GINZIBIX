import os
import glob
import queue
import shutil
from .par2lib import calc_file_md5hash
import subprocess
import re
import time
import pexpect
import inotify_simple
import signal
import sys

lpref = __name__ + " - "


class SigHandler_Verifier:
    def __init__(self, logger):
        self.logger = logger

    def sighandler_verifier(self, a, b):
        self.logger.info(lpref + "terminated!")
        sys.exit()


def par_verifier(mp_outqueue, renamed_dir, verifiedrar_dir, main_dir, logger, pwdb, nzbname, pvmode):
    sh = SigHandler_Verifier(logger)
    signal.signal(signal.SIGINT, sh.sighandler_verifier)
    signal.signal(signal.SIGTERM, sh.sighandler_verifier)

    logger.info(lpref + "starting rar_verifier")
    logger.debug(lpref + "dirs: " + renamed_dir + " / " + verifiedrar_dir)
    if pvmode == "verify":
        p2 = pwdb.get_renamed_p2(renamed_dir, nzbname)

    pwdb.db_nzb_update_verify_status(nzbname, 1)
    # a: verify all unverified files in "renamed"
    unverified_rarfiles = pwdb.get_all_renamed_rar_files()
    doloadpar2vols = False
    if pvmode == "verify" and not p2:
        logger.info(lpref + "no par2 file found")
    if pvmode == "verify" and unverified_rarfiles and p2:
        logger.info(lpref + "verifying all unchecked rarfiles")
        for f0 in unverified_rarfiles:
            filename = f0.renamed_name
            md5 = calc_file_md5hash(renamed_dir + filename)
            md5match = [(pmd5 == md5) for pname, pmd5 in p2.filenames() if pname == filename]
            logger.info(lpref + "p2 md5: " + filename + " = " + str(md5match))
            if False in md5match:
                logger.warning(lpref + "error in 'p2 md5' for file " + filename)
                pwdb.db_file_update_parstatus(f0.orig_name, -1)
                doloadpar2vols = True
            else:
                logger.info(lpref + "copying " + filename + " to " + verifiedrar_dir)
                shutil.copy(renamed_dir + filename, verifiedrar_dir)
                pwdb.db_file_update_parstatus(f0.orig_name, 1)
    if pvmode == "copy":
        logger.info(lpref + "copying all rarfiles")
        for f0 in unverified_rarfiles:
            filename = f0.renamed_name
            logger.info(lpref + "copying " + filename + " to " + verifiedrar_dir)
            shutil.copy(renamed_dir + filename, verifiedrar_dir)
            pwdb.db_file_update_parstatus(f0.orig_name, 1)
    if doloadpar2vols:
        mp_outqueue.put(doloadpar2vols)

    # b: inotify renamed_dir
    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(renamed_dir, watch_flags)

    while True:
        allparstatus = pwdb.db_file_getallparstatus(0)
        if 0 not in allparstatus:
            logger.info(lpref + "All renamed rars checked, exiting ...")
            break
        events = get_inotify_events(inotify)
        if events or 0 in allparstatus:
            if pvmode == "verify" and not p2:
                p2 = pwdb.get_renamed_p2(renamed_dir, nzbname)
            if pvmode == "verify" and p2:
                # logger.debug("-----------------------------------------------------")
                for rar in glob.glob(renamed_dir + "*.rar"):
                    rar0 = rar.split("/")[-1]
                    f0 = pwdb.db_file_get_renamed(rar0)
                    if not f0:
                        continue
                    if pwdb.db_file_getparstatus(rar0) == 0 and f0.renamed_name != "N/A":
                        md5 = calc_file_md5hash(renamed_dir + rar0)
                        md5match = [(pmd5 == md5) for pname, pmd5 in p2.filenames() if pname == f0.renamed_name]
                        if False in md5match:
                            logger.warning(lpref + "error in 'p2 md5' for file " + f0.renamed_name)
                            pwdb.db_file_update_parstatus(f0.orig_name, -1)
                            if not doloadpar2vols:
                                doloadpar2vols = True
                                mp_outqueue.put(doloadpar2vols)
                        else:
                            logger.info(lpref + "p2 md5' CORRECT for file " + f0.renamed_name + ", copying to " + verifiedrar_dir)
                            shutil.copy(renamed_dir + f0.renamed_name, verifiedrar_dir)
                            pwdb.db_file_update_parstatus(f0.orig_name, 1)
            if pvmode == "copy":
                rar0 = rar.split("/")[-1]
                f0 = pwdb.db_file_get_renamed(rar0)
                if not f0:
                    continue
                if pwdb.db_file_getparstatus(rar0) == 0 and f0.renamed_name != "N/A":
                    logger.info(lpref + f0.renamed_name + ": copying to " + verifiedrar_dir)
                    shutil.copy(renamed_dir + f0.renamed_name, verifiedrar_dir)
                    pwdb.db_file_update_parstatus(f0.orig_name, 1)
        if pwdb.db_only_verified_rars():
            break
        time.sleep(1)

    par2name = pwdb.db_get_renamed_par2(nzbname)
    corruptrars = pwdb.get_all_corrupt_rar_files()
    if par2name and corruptrars:
        logger.info(lpref + "par2vol files present, repairing ...")
        res0 = multipartrar_repair(renamed_dir, par2name, logger)
        if res0 == 1:
            logger.info(lpref + "repair success")
            pwdb.db_nzb_update_verify_status(nzbname, 2)
            # copy all no yet copied rars to verifiedrar_dir
            for c in corruptrars:
                logger.info(lpref + "copying " + c.renamed_name + " to verifiedrar_dir")
                pwdb.db_file_update_parstatus(c.orig_name, 1)
                pwdb.db_file_update_status(c.orig_name, 2)
                shutil.copy(renamed_dir + c.renamed_name, verifiedrar_dir)
        else:
            logger.error(lpref + "repair failed!")
            pwdb.db_nzb_update_verify_status(nzbname, -1)
            for c in corruptrars:
                pwdb.db_file_update_parstatus(c.orig_name, -2)
    else:
        pwdb.db_nzb_update_verify_status(nzbname, 2)


def get_inotify_events(inotify):
    events = []
    for event in inotify.read(timeout=1):
        is_created_file = False
        str0 = event.name
        flgs0 = []
        for flg in inotify_simple.flags.from_mask(event.mask):
            if "flags.CREATE" in str(flg) and "flags.ISDIR" not in str(flg):
                flgs0.append(str(flg))
                is_created_file = True
        if not is_created_file:
            continue
        else:
            events.append((str0, flgs0))
    return events


def par_verifier0(mp_inqueue, mp_outqueue, download_dir, verifiedrar_dir, main_dir, logger, filetypecounter, p2=None):
    if not os.path.isdir(verifiedrar_dir):
        os.mkdir(verifiedrar_dir)
    maxrar = filetypecounter["rar"]["max"]
    logger.info("PAR_VERIFIER > Starting!")
    # phase I: verify
    rarlist = []
    while True:
        # get from inqueue
        res0 = None
        while True:
            try:
                res0 = mp_inqueue.get_nowait()
            except queue.Empty:
                break
        if res0:
            p2_0 = res0
            if p2_0 == -1:
                logger.warning("PAR_VERIFIER > Received signal to stop from main!")
                break
            # 0 ... not tested
            # -1 ... not ok (yet)
            # 1 ... tested & ok
            if p2_0:
                p2 = p2_0

        doloadpar2vols = False
        verifiedstatus = 0  # 0 .. still running, 1 .. all ok/finished, -1 .. not ok/finished
        corruptrars = []
        rarf = [r.split("/")[-1] for r in glob.glob(download_dir + "*.rar")]

        for r in rarf:
            if r not in [rn for rn, isok in rarlist]:
                rarlist.append((r, 0))
        # if no par2 given have to test with "unrar t"
        if not p2:
            for i, (filename, isok) in enumerate(rarlist):
                if os.path.isfile(verifiedrar_dir + filename):
                    continue
                if filename in rarf and isok == 0 or isok == -1:
                    res0 = multipartrar_test(download_dir, filename, logger)
                    logger.info("PAR_VERIFIER > unrar t: " + filename + " = " + str(res0))
                    rarlist[i] = filename, res0
                    if res0 == 1:
                        logger.info("PAR_VERIFIER > copying " + filename + " to " + verifiedrar_dir)
                        shutil.copy(download_dir + filename, verifiedrar_dir)
                    else:
                        corruptrars.append(((filename, isok)))
                        logger.warning("PAR_VERIFIER > error in 'unrar t' for file " + filename)
                        doloadpar2vols = True
        else:
            for i, (filename, isok) in enumerate(rarlist):
                if os.path.isfile(verifiedrar_dir + filename) or isok == -1:
                    continue
                if filename in rarf and isok == 0:
                    md5 = calc_file_md5hash(download_dir + filename)
                    md5match = [(pmd5 == md5) for pname, pmd5 in p2.filenames() if pname == filename]
                    logger.info("PAR_VERIFIER > p2 md5: " + filename + " = " + str(md5match))
                    if False in md5match:
                        rarlist[i] = filename, -1
                        corruptrars.append(((filename, md5, -1)))
                        logger.warning("PAR_VERIFIER > error in 'p2 md5' for file " + filename)
                        doloadpar2vols = True
                    else:
                        rarlist[i] = filename, 1
                        logger.info("PAR_VERIFIER > copying " + filename + " to " + verifiedrar_dir)
                        shutil.copy(download_dir + filename, verifiedrar_dir)
        if doloadpar2vols:
            mp_outqueue.put((doloadpar2vols, -9999))
        # if all checked is there still a fill which is not ok??
        if len(rarf) == maxrar and len(rarlist) == maxrar:
            allok = True
            for (filename, isok) in rarlist:
                if isok != 1:
                    allok = False
                    break
            verifiedstatus = 1
            if not allok:
                verifiedstatus = -1
            break
        time.sleep(0.5)

    logger.info("PAR_VERIFIER > rar verify status is " + str(verifiedstatus))

    # phase II: repair
    if corruptrars or verifiedstatus == -1:
        logger.info("PAR_VERIFIER > Starting repair")
        rf = [(r, re.search(r"[.]par2$", r.split("/")[-1], flags=re.IGNORECASE)) for r in glob.glob(download_dir + "*")]
        rf_par2 = [r for r, rs in rf if rs is not None]
        if rf_par2:
            rf_0 = [(r, re.search(r"vol[0-9][0-9]*[+]", r, flags=re.IGNORECASE)) for r in rf_par2]
            rf_par2vol = [r for r, rs in rf_0 if rs is not None]
            if rf_par2vol:
                logger.info("PAR_VERIFIER > par2vol files present, repairing ...")
                res0 = multipartrar_repair(download_dir, rf_par2vol[0], logger)
                if res0 == 1:
                    logger.info("PAR_VERIFIER > repair success")
                    # copy all no yet copied rars to verifiedrar_dir
                    rf_0 = [(r, re.search(r"[.]rar$", r, flags=re.IGNORECASE)) for r in glob.glob(download_dir + "*")]
                    rars_in_downloaddir = [r.split("/")[-1] for r, rs in rf_0 if rs is not None]
                    files_in_verifieddir = [r.split("/")[-1] for r in glob.glob(verifiedrar_dir + "*")]
                    for filename in rars_in_downloaddir:
                        if filename not in files_in_verifieddir:
                            logger.info("PAR_VERIFIER > copying " + filename + " to verifiedrar_dir")
                            shutil.copy(download_dir + filename, verifiedrar_dir)
                        # delete all rars in download_dir
                    verifiedstatus = 1
                else:
                    logger.error("PAR_VERIFIER > repair failed!")
            else:
                logger.warning("PAR_VERIFIER > No par2vol files present, cannot repair!")
                verifiedstatus = -1
        else:
            logger.warning("PAR_VERIFIER > no par files exist!")
            verifiedstatus = -1
    else:
        logger.info("PAR_VERIFIER > All files ok, no repair needed!")
        verifiedstatus = 1

    logger.warning("PAR_VERIFIER > Exiting par_verifier!")
    mp_outqueue.put((-9999, verifiedstatus))


def multipartrar_test(directory, rarname0, logger):
    rarnames = []
    sortedrarnames = []
    cwd0 = os.getcwd()
    os.chdir(directory)
    for r in glob.glob("*.rar"):
        rarnames.append(r)
    for r in rarnames:
        rarnr = r.split(".part")[-1].split(".rar")[0]
        sortedrarnames.append((int(rarnr), r))
    sortedrarnames = sorted(sortedrarnames, key=lambda nr: nr[0])
    rar0_nr, rar0_nm = [(nr, rarn) for (nr, rarn) in sortedrarnames if rarn == rarname0][0]
    ok_sorted = True
    for i, (nr, rarnr) in enumerate(sortedrarnames):
        if i + 1 == rar0_nr:
            break
        if i + 1 != nr:
            ok_sorted = False
            break
    if not ok_sorted:
        # print(-1)
        return -1              # -1 cannot check, rar in between is missing
    # ok sorted, unrar t
    cmd = "unrar t " + rarname0
    child = pexpect.spawn(cmd)
    str0 = []
    str00 = ""
    status = 1

    while True:
        try:
            a = child.read_nonblocking().decode("utf-8")
            if a == "\n":
                if str00:
                    str0.append(str00)
                    str00 = ""
            if ord(a) < 32:
                continue
            str00 += a
        except pexpect.exceptions.EOF:
            break
    # logger.info("MULTIPARTRAR_TEST > " + str(str0))
    for i, s in enumerate(str0):
        if rarname0 in s:
            try:
                if "- checksum error" in str0[i + 2]:
                    status = -2
                if "Cannot find" in s:
                    status = -1
            except Exception as e:
                logger.info("MULTIPARTRAR_TEST > " + str(e))
                status = -1

    os.chdir(cwd0)
    return status


def multipartrar_repair(directory, parvolname, logger):
    cwd0 = os.getcwd()
    os.chdir(directory)
    logger.warning(lpref + "checking if repair possible ...")
    ssh = subprocess.Popen(['par2verify', parvolname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
    sshres = ssh.stdout.readlines()
    repair_is_required = False
    repair_is_possible = False
    exitstatus = 0
    for ss in sshres:
        ss0 = ss.decode("utf-8")
        if "Repair is required" in ss0:
            repair_is_required = True
        if "Repair is possible" in ss0:
            repair_is_possible = True
    if repair_is_possible and repair_is_required:
        logger.info(lpref + "repair is required and possible, performing par2repair ...")
        # repair
        ssh = subprocess.Popen(['par2repair', parvolname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
        sshres = ssh.stdout.readlines()
        repair_complete = False
        for ss in sshres:
            ss0 = ss.decode("utf-8")
            if "Repair complete" in ss0:
                repair_complete = True
        if not repair_complete:
            exitstatus = -1
            logger.error(lpref + "could not repair")
        else:
            logger.info(lpref + "repair success!!")
            exitstatus = 1
    elif repair_is_required and not repair_is_possible:
        logger.error(lpref + "repair is required but not possible!")
        exitstatus = -1
    elif not repair_is_required and not repair_is_possible:
        logger.error(lpref + "repair is not required - all OK!")
        exitstatus = 1
    os.chdir(cwd0)
    return exitstatus

