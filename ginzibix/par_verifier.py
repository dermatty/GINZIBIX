import os
import glob
import shutil
from .par2lib import calc_file_md5hash
import subprocess
import sys
import time
import pexpect
import inotify_simple
import signal
from setproctitle import setproctitle

from ginzibix.mplogging import whoami
from ginzibix import mplogging
from ginzibix import PWDBSender, make_dirs, mpp_is_alive, mpp_join, GUI_Poller, get_cut_nzbname, get_cut_msg, get_bg_color, get_status_name_and_color,\
    clear_postproc_dirs, get_server_config, get_configured_servers, get_config_for_server, get_free_server_cfg, is_port_in_use, do_mpconnections,\
    kill_mpp


TERMINATED = False
TEST_FOR_FAILED_RAR = False


class SigHandler_Verifier:
    def __init__(self, logger):
        self.logger = logger

    def sighandler_verifier(self, a, b):
        global TERMINATED
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


def get_no_of_blocks(par2file, renamed_dir, fn0):
    ssh = subprocess.Popen(['par2verify', renamed_dir + par2file], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
    sshres = ssh.stdout.readlines()
    if not sshres:
        return True, 99999
    damaged = False
    rec_blocks_needed = 0
    for ss in sshres:
        ss0 = ss.decode("utf-8")
        if fn0 in ss0 and "damaged. Found" in ss0:
            damaged = True
            datablocklist = [int(s) for s in ss0.split() if s.isdigit()]
            try:
                found_blocks = datablocklist[0]
                sum_blocks = datablocklist[-1]
                rec_blocks_needed = sum_blocks - found_blocks
            except Exception:
                rec_blocks_needed = -1
            break
    return damaged, rec_blocks_needed


class P2:
    def __init__(self, p2list):
        self.p2list = p2list
        self.allfilenames = self.getfilenames()

    def getfilenames(self):
        allfilenames = []
        for p2, _, _, _ in self.p2list:
            for pname, pmd5 in p2.filenames():
                allfilenames.append((pname, pmd5))
        return allfilenames

    def filenames(self):
        return self.allfilenames


def par_verifier(child_pipe, renamed_dir, verifiedrar_dir, main_dir, mp_loggerqueue, nzbname, pvmode, event_idle, cfg):
    setproctitle("gzbx." + os.path.basename(__file__))
    logger = mplogging.setup_logger(mp_loggerqueue, __file__)
    logger.debug(whoami() + "starting ...")
    sh = SigHandler_Verifier(logger)
    signal.signal(signal.SIGINT, sh.sighandler_verifier)
    signal.signal(signal.SIGTERM, sh.sighandler_verifier)

    pwdb = PWDBSender()
    event_idle.clear()

    if pvmode == "verify":
        # p2 = pwdb.get_renamed_p2(renamed_dir, nzbname)
        try:
            p2list = pwdb.exc("db_p2_get_p2list", [nzbname], {})
            p2 = P2(p2list)
        except Exception as e:
            logger.warning(whoami() + str(e))

    # pwdb.db_nzb_update_verify_status(nzbname, 1)
    pwdb.exc("db_nzb_update_verify_status", [nzbname, 1], {})

    # a: verify all unverified files in "renamed"
    unverified_rarfiles = None
    try:
        # unverified_rarfiles = pwdb.get_all_renamed_rar_files(nzbname)
        unverified_rarfiles = pwdb.exc("get_all_renamed_rar_files", [nzbname], {})
    except Exception as e:
        logger.debug(whoami() + str(e) + ": no unverified rarfiles met in first run, skipping!")
    doloadpar2vols = False
    if pvmode == "verify" and not p2:
        logger.debug(whoami() + "no par2 file found")
    if pvmode == "verify" and unverified_rarfiles and p2:
        logger.debug(whoami() + "verifying all unchecked rarfiles")
        for filename, f_origname in unverified_rarfiles:
            f_short = filename.split("/")[-1]
            md5 = calc_file_md5hash(renamed_dir + filename)
            md5match = [(pmd5 == md5) for pname, pmd5 in p2.filenames() if pname == filename]
            if False in md5match:
                logger.warning(whoami() + " error in md5 hash match for file " + f_short)
                pwdb.exc("db_msg_insert", [nzbname, "error in md5 hash match for file " + f_short, "warning"], {})
                pwdb.exc("db_nzb_update_verify_status", [nzbname, -2], {})
                pwdb.exc("db_file_update_parstatus", [f_origname, -1], {})
                child_pipe.send(True)
            else:
                logger.info(whoami() + f_short + " md5 hash match ok, copying to verified_rar dir")
                pwdb.exc("db_msg_insert", [nzbname, f_short + " md5 hash match ok, copying to verified_rar dir ", "info"], {})
                shutil.copy(renamed_dir + filename, verifiedrar_dir)
                pwdb.exc("db_file_update_parstatus", [f_origname, 1], {})
    elif (pvmode == "verify" and not p2) or (pvmode == "copy"):
        logger.info(whoami() + "copying all rarfiles")
        for filename, f_origname in unverified_rarfiles:
            f_short = filename.split("/")[-1]
            sfvcheck = pwdb.exc("db_nzb_check_sfvcrc32", [nzbname, renamed_dir, renamed_dir + filename], {})
            if sfvcheck == -1:
                logger.warning(whoami() + " error in crc32 check for file " + f_short)
                pwdb.exc("db_msg_insert", [nzbname, "error in crc32 check for file " + f_short, "warning"], {})
                pwdb.exc("db_nzb_update_verify_status", [nzbname, -2], {})
                pwdb.exc("db_file_update_parstatus", [f_origname, -1], {})
                child_pipe.send(True)
                continue
            logger.debug(whoami() + "copying " + f_short + " to verified_rar dir")
            pwdb.exc("db_msg_insert", [nzbname, "copying " + f_short + " to verified_rar dir ", "info"], {})
            shutil.copy(renamed_dir + filename, verifiedrar_dir)
            pwdb.exc("db_file_update_parstatus", [f_origname, 1], {})

    # b: inotify renamed_dir
    inotify = inotify_simple.INotify()
    watch_flags = inotify_simple.flags.CREATE | inotify_simple.flags.DELETE | inotify_simple.flags.MODIFY | inotify_simple.flags.DELETE_SELF
    inotify.add_watch(renamed_dir, watch_flags)

    while not TERMINATED:
        # allparstatus = pwdb.db_file_getallparstatus(0)
        allparstatus = pwdb.exc("db_file_getallparstatus", [0], {})
        if 0 not in allparstatus:
            event_idle.clear()
            logger.info(whoami() + "all renamed rars checked, exiting par_verifier")
            break
        events = get_inotify_events(inotify)
        event_idle.set()
        if events or 0 in allparstatus:
            event_idle.clear()
            if pvmode == "verify" and not p2:
                try:
                    p2list = pwdb.exc("db_p2_get_p2list", [nzbname], {})
                    p2 = P2(p2list)
                except Exception as e:
                    logger.debug(whoami() + str(e))
            if pvmode == "verify" and p2:
                for rar in glob.glob(renamed_dir + "*") + glob.glob(renamed_dir + ".*"):
                    rar0 = rar.split("/")[-1]
                    f0 = pwdb.exc("db_file_get_renamed", [rar0], {})
                    if not f0:
                        continue
                    f0_origname, f0_renamedname, f0_ftype = f0
                    #print(f0_renamedname, " : ", p2.filenames())
                    #print("-" * 80)
                    if not f0_ftype == "rar":
                        continue
                    if pwdb.exc("db_file_getparstatus", [rar0], {}) == 0 and f0_renamedname != "N/A":
                        f_short = f0_renamedname.split("/")[-1]
                        md5 = calc_file_md5hash(renamed_dir + rar0)
                        md5match = [(pmd5 == md5) for pname, pmd5 in p2.filenames() if pname == f0_renamedname]
                        if False in md5match:
                            logger.warning(whoami() + "error in md5 hash match for file " + f_short)
                            pwdb.exc("db_msg_insert", [nzbname, "error in md5 hash match for file " + f_short, "warning"], {})
                            pwdb.exc("db_nzb_update_verify_status", [nzbname, -2], {})
                            pwdb.exc("db_file_update_parstatus", [f0_origname, -1], {})
                            child_pipe.send(True)
                        else:
                            logger.info(whoami() + f_short + " md5 hash match ok, copying to verified_rar dir")
                            pwdb.exc("db_msg_insert", [nzbname, f_short + " md5 hash match ok, copying to verified_rar dir ", "info"], {})
                            shutil.copy(renamed_dir + f0_renamedname, verifiedrar_dir)
                            pwdb.exc("db_file_update_parstatus", [f0_origname, 1], {})
            # free rars or copy mode?
            elif (pvmode == "verify" and not p2) or (pvmode == "copy"):
                # maybe we can check via sfv file?
                for file0full in glob.glob(renamed_dir + "*") + glob.glob(renamed_dir + ".*"):
                    file0short = file0full.split("/")[-1]
                    ft = pwdb.exc("db_file_getftype_renamed", [file0short], {})
                    if ft == "rar":
                        rar0 = file0short
                        f0 = pwdb.exc("db_file_get_renamed", [rar0], {})
                        if not f0:
                            continue
                        f0_origname, f0_renamedname, f0_ftype = f0
                        sfvcheck = pwdb.exc("db_nzb_check_sfvcrc32", [nzbname, renamed_dir, file0full], {})
                        if sfvcheck == -1:
                            logger.warning(whoami() + " error in crc32 check for file " + rar0)
                            pwdb.exc("db_msg_insert", [nzbname, "error in crc32 check for file " + rar0, "warning"], {})
                            pwdb.exc("db_nzb_update_verify_status", [nzbname, -2], {})
                            pwdb.exc("db_file_update_parstatus", [f0_origname, -1], {})
                            child_pipe.send(True)
                            continue
                        if pwdb.exc("db_file_getparstatus", [rar0], {}) == 0 and f0_renamedname != "N/A":
                            logger.debug(whoami() + "copying " + f0_renamedname.split("/")[-1] + " to verified_rar dir")
                            pwdb.exc("db_msg_insert", [nzbname, "copying " + f0_renamedname.split("/")[-1] + " to verified_rar dir", "info"], {})
                            shutil.copy(renamed_dir + f0_renamedname, verifiedrar_dir)
                            pwdb.exc("db_file_update_parstatus", [f0_origname, 1], {})
        allrarsverified, rvlist = pwdb.exc("db_only_verified_rars", [nzbname], {})
        if allrarsverified:
            break
        time.sleep(1)

    if TERMINATED:
        logger.info(whoami() + "terminated!")
        sys.exit()

    logger.debug(whoami() + "all rars are checked!")
    corruptrars = pwdb.exc("get_all_corrupt_rar_files", [nzbname], {})
    if not corruptrars:
        logger.debug(whoami() + "rar files ok, no repair needed, exiting par_verifier")
        pwdb.exc("db_nzb_update_verify_status", [nzbname, 2], {})
    elif p2list and corruptrars:
        pwdb.exc("db_msg_insert", [nzbname, "repairing rar files", "info"], {})
        logger.info(whoami() + "par2vol files present, repairing ...")
        allok = True
        allfound = True
        corruptrars_1 = [c1 for c1, _ in corruptrars]
        corruptrars_2 = [c2 for _, c2 in corruptrars]
        for _, fnshort, fnlong, rarfiles in p2list:
            rarf_match = [rarf for rarf, _ in rarfiles if rarf in corruptrars_1 or rarf in corruptrars_2]
            if len(rarf_match) == 0:
                allfound = False
                continue
            lrar = str(len(rarfiles))
            pwdb.exc("db_msg_insert", [nzbname, "performing par2verify for " + fnshort, "info"], {})
            ssh = subprocess.Popen(['par2verify', fnlong], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
            sshres = ssh.stdout.readlines()
            repair_is_required = False
            repair_is_possible = False
            for ss in sshres:
                ss0 = ss.decode("utf-8")
                if "Repair is required" in ss0:
                    repair_is_required = True
                if "Repair is possible" in ss0:
                    repair_is_possible = True
            if not repair_is_required:
                pwdb.exc("db_msg_insert", [nzbname, "par2verify for " + fnshort + ": repair is not required!", "info"], {})
                res0 = 1
            elif repair_is_required and not repair_is_possible:
                pwdb.exc("db_msg_insert", [nzbname, "par2verify for " + fnshort + ": repair is required but not possible", "error"], {})
                res0 = -1
            elif repair_is_required and repair_is_possible:
                pwdb.exc("db_msg_insert", [nzbname, "par2verify for " + fnshort + ": repair is required and possible, repairing files if possible", "info"], {})
                res0 = multipartrar_repair(renamed_dir, fnshort, pwdb, nzbname, logger)
            else:
                res0 = -1
            if res0 != 1:
                allok = False
                logger.error(whoami() + "repair failed for " + lrar + "rarfiles in " + fnshort)
                pwdb.exc("db_msg_insert", [nzbname, "rar file repair failed for " + lrar + " rarfiles in " + fnshort + "!", "error"], {})
            else:
                logger.info(whoami() + "repair success for " + lrar + "rarfiles in " + fnshort)
                pwdb.exc("db_msg_insert", [nzbname, "rar file repair success for " + lrar + " rarfiles in " + fnshort + "!", "info"], {})
        if not allfound:
            allok = False
            logger.error(whoami() + "cannot attempt one or more par2repairs due to missing par2 file(s)!")
            pwdb.exc("db_msg_insert", [nzbname, "cannot attempt one or more par2repairs due to missing par2 file(s)!", "error"], {})
        if allok:
            logger.info(whoami() + "repair success")
            pwdb.exc("db_nzb_update_verify_status", [nzbname, 2], {})
            # copy all no yet copied rars to verifiedrar_dir
            for c_origname, c_renamedname in corruptrars:
                logger.info(whoami() + "copying " + c_renamedname + " to verifiedrar_dir")
                pwdb.exc("db_file_update_parstatus", [c_origname, 1], {})
                pwdb.exc("db_file_update_status", [c_origname, 2], {})
                shutil.copy(renamed_dir + c_renamedname, verifiedrar_dir)
        else:
            logger.error(whoami() + "repair failed!")
            pwdb.exc("db_nzb_update_verify_status", [nzbname, -1], {})
            for _, c_origname in corruptrars:
                pwdb.exc("db_file_update_parstatus", [c_origname, -2], {})
    else:
        pwdb.exc("db_msg_insert", ["nzbname", "rar file repair failed, no par files available", "error"], {})
        logger.warning(whoami() + "some rars are corrupt but cannot repair (no par2 files)")
        pwdb.exc("db_nzb_update_verify_status", [nzbname, -1], {})
    logger.info(whoami() + "terminated!")
    sys.exit()


def get_inotify_events(inotify):
    events = []
    for event in inotify.read(timeout=500):
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


def multipartrar_test(directory, rarname0, logger):
    rarnames = []
    sortedrarnames = []
    cwd0 = os.getcwd()
    os.chdir(directory)
    for r in glob.glob("*.rar") + glob.glob(".*.rar"):
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


def multipartrar_repair(directory, parvolname, pwdb, nzbname, logger):
    cwd0 = os.getcwd()
    os.chdir(directory)
    logger.info(whoami() + "checking if repair possible for " + parvolname)
    pwdb.exc("db_msg_insert", [nzbname, "checking if repair is possible", "info"], {})
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
        logger.info(whoami() + "repair is required and possible, performing par2repair")
        pwdb.exc("db_msg_insert", [nzbname, "repair is required and possible, performing par2repair", "info"], {})
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
            logger.error(whoami() + "could not repair")
        else:
            logger.info(whoami() + "repair success!!")
            exitstatus = 1
    elif repair_is_required and not repair_is_possible:
        logger.error(whoami() + "repair is required but not possible!")
        pwdb.exc("db_msg_insert", [nzbname, "repair is required but not possible!", "error"], {})
        exitstatus = -1
    elif not repair_is_required and not repair_is_possible:
        logger.error(whoami() + "repair is not required - all OK!")
        exitstatus = 1
    os.chdir(cwd0)
    return exitstatus


'''fn = "/home/stephan/.ginzibix/test/pTr21K8XimPtko.par2"
ssh = subprocess.Popen(['par2verify', fn], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
sshres = ssh.stdout.readlines()
print("****")
fn0 = "pTr21K8XimPtko.part1.rar"
damaged = False
rec_blocks_needed = 0
for ss in sshres:
    ss0 = ss.decode("utf-8")
    if fn0 in ss0 and "damaged" in ss0:
        damaged = True
    if damaged and "You need" in ss0 and "more recovery blocks to be able to repair":
        rec_blocks_needed = [int(s) for s in ss0.split() if s.isdigit()]
        break
print(damaged, rec_blocks_needed)'''
