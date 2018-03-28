import os
import glob
import queue
import shutil
from .par2lib import calc_file_md5hash
import subprocess
import re
import time
import pexpect


def par_verifier(mp_inqueue, mp_outqueue, download_dir, verifiedrar_dir, main_dir, logger, filetypecounter, p2=None):
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
    logger.warning("MULTIRAR_REPAIR > checking if repair possible ...")
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
        logger.info("MULTIRAR_REPAIR > Repair is required and possible, performing par2repair ...")
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
            logger.error("MULTIRAR_REPAIR > Could not repair")
        else:
            logger.info("MULTIRAR_REPAIR > Repair success!!")
            exitstatus = 1
    elif repair_is_required and not repair_is_possible:
        logger.error("MULTIRAR_REPAIR > Repair is required but not possible!")
        exitstatus = -1
    elif not repair_is_required and not repair_is_possible:
        logger.error("MULTIRAR_REPAIR > Repair is not required - all OK!")
        exitstatus = 1
    os.chdir(cwd0)
    return exitstatus
