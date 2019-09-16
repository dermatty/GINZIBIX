import os
import subprocess
import re
import logging
import logging.handlers
import sys
import glob
import pexpect
import signal


from ginzibix.mplogging import whoami
from ginzibix import par2lib


def get_sorted_rar_list(directory):
    rarlist = []
    minnr = -1000
    i = 1
    for rarf in glob.glob(directory + "*") + glob.glob(directory + ".*"):
        ftype = par2lib.get_file_type(rarf)
        # gg = re.search(r"[.]rar", rarf, flags=re.IGNORECASE)
        if ftype == "rar":
            # first the easy way: *.part01.rar
            gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
            if gg:
                rarlist.append((int(gg.group().split(".")[0]), rarf))
                continue
            # then: only .rar
            gg = re.search(r"[.]rar", rarf, flags=re.IGNORECASE)
            if gg:
                rarlist.append((minnr, rarf))
                minnr += 1
                continue
            # then: ".r00"
            gg = re.search(r"[.]r+[0-9]*", rarf, flags=re.IGNORECASE)
            if gg:
                try:
                    nr = int(gg.group().split(".r")[-1])
                    rarlist.append((nr, rarf))
                    continue
                except Exception:
                    pass
            # any other rar-wise file, no idea about sorting, let's hope the best
            rarlist.append((i, rarf))
            i += 1
            # rarlist.append((int(gg.group().split(".")[0]), rarf))
    rar_sortedlist = []
    if rarlist:
        rar_sortedlist = sorted(rarlist, key=lambda nr: nr[0])
    return rar_sortedlist

# attention:
#      test if password protected: unrar vt -p- TSaB2G156300048493oCx190713WAY.part01.rar
#               UNRAR 5.71 beta 1 freeware      Copyright (c) 1993-2019 Alexander Roshal
#                The specified password is incorrect.
#                Archive: TSaB2G156300048493oCx190713WAY.part01.rar
#                Details: RAR 5, encrypted headers
#      test for corr. password: unrar vt -p7WbVqhchfuxHhth TSaB2G156300048493oCx190713WAY.part01.rar0
'''              UNRAR 5.71 beta 1 freeware      Copyright (c) 1993-2019 Alexander Roshal
                 Archive: TSaB2G156300048493oCx190713WAY.part01.rar
                 Details: RAR 5, volume 1, recovery record, lock, encrypted headers

                         Name: the.sisters.and.brothers.2018.german.dl.1080p.web.h264-wayne.nfo
                         Type: File
                         Size: 282
                  Packed size: 288
                        Ratio: 102%
                        mtime: 2019-07-11 22:50:26,000000000
                   Attributes: -rwxrwx---
                        CRC32: 984C3E00
                      Host OS: Unix
                  Compression: RAR 5.0(v50) -m0 -md=128K
                        Flags: encrypted 

                         Name: the.sisters.and.brothers.2018.german.dl.1080p.web.h264-wayne.mkv
                         Type: File
                         Size: 9271299836
                  Packed size: 243505850
                        Ratio: -->
                        mtime: 2019-07-12 07:34:32,000000000
                   Attributes: -rwxrwx---
                   Pack-CRC32: 82EC7A3B
                      Host OS: Unix
                  Compression: RAR 5.0(v50) -m0 -md=128K
                        Flags: encrypted'''



def is_rar_password_protected(directory, logger):
    # return value:
    #    1 ... is pw protected
    #    0 ... is not a rar file
    #    -1 .. is not pw protected
    #    -2 .. no rars in dir!
    #    -3 .. do restart (first volume in archive not found)
    cwd0 = os.getcwd()
    if directory[-1] != "/":
        directory += "/"
    rars = get_sorted_rar_list(directory)
    if not rars:
        return -2
    os.chdir(directory)
    do_restart = False
    for _, rarname0 in rars:
        rarname = rarname0.split("/")[-1]
        ssh = subprocess.Popen(["unrar", "t", "-p-", rarname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
        ssherr = ssh.stderr.readlines()
        is_pw_protected = False
        do_restart = False
        for ss in ssherr:
            ss0 = ss.decode("utf-8")
            if "You need to start extraction" in ss0:
                do_restart = True
                break
            if "Corrupt file or wrong password" in ss0 or "password is incorrect" in ss0:
                logger.info(rarname + " is password protected")
                is_pw_protected = True
                do_restart = False
                break
        if not do_restart:
            break

    os.chdir(cwd0)
    if do_restart:
        return -3
    elif is_pw_protected:
        return 1
    else:
        return -1


def test_password(pw, rarname):
    child = pexpect.spawn("unrar t " + rarname + " -p" + pw)
    str0 = ""
    ctr = 0
    allok = False
    do2ndtry = False
    while True:
        try:
            a = child.read_nonblocking().decode("utf-8")
            str0 += a
        except pexpect.exceptions.EOF:
            break
        except pexpect.exceptions.TIMEOUT:
            break
        except Exception:
            # this is possibly a 'utf-8' cant't decode error
            # if this happens try with possibly slower approach
            do2ndtry = True
            break
        if "\r\n" in str0:
            if "All OK" in str0:
                allok = True
                break
            if "Testing archive" in str0:
                ctr += 1
                if ctr > 2:
                    allok = True
                    break
            str0 = ""
    child.kill(signal.SIGKILL)
    if do2ndtry:
        allok = test_password2(pw, rarname)
    return allok


# if there was a utf-8 can't decode error:
#        do unrar test for all rars and eval finally
def test_password2(pw, rarname):
    child = pexpect.spawn("unrar t " + rarname + " -p" + pw)
    str0 = ""
    byt0 = b""

    firstpassok = True
    while True:
        try:
            a_raw = child.read_nonblocking()
            byt0 += a_raw
        except pexpect.exceptions.EOF:
            break
        except pexpect.exceptions.TIMEOUT:
            firstpassok = False
            break
        except Exception:
            firstpassok = False
            break

    child.kill(signal.SIGKILL)
    allok = False

    if firstpassok:
        try:
            str0 = byt0.decode("utf-8")
            strarr = str0.split("\r\n")
        except Exception:
            strarr = []
        for sa in strarr:
            if "All OK" in sa:
                allok = True
                break
    return allok


def test_passwordV1(pw, rarname):
    ssh = subprocess.Popen(["unrar", "t", "-p" + pw, rarname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
    ssherr = ssh.stderr.readlines()
    return (not ssherr)


def get_password(directory, pw_file, nzbname0, logger, get_pw_direct=False):
    if directory[-1] != "/":
        directory += "/"
    rars = get_sorted_rar_list(directory)
    if not rars:
        return None
    rarname0 = rars[0][1]
    rarname = rarname0.split("/")[-1]
    nzbname = nzbname0.split(".nzb")[0]

    logger.debug(whoami() + "trying to get password")

    cwd0 = os.getcwd()

    if get_pw_direct:
        gg = re.search(r"}}.nzb$", nzbname0, flags=re.IGNORECASE)
        if gg:
            try:
                pw0 = nzbname0[:gg.start()].split("{{")[-1]
                os.chdir(directory)
                if test_password(pw0, rarname):
                    logger.info(whoami() + "Found PW for NZB " + nzbname + ": " + pw0)
                    os.chdir(cwd0)
                    return pw0
                else:
                    logger.warning(whoami() + "provided password was not correct!")
            except Exception as e:
                logger.debug(whoami() + str(e) + ": cannot get pw from nzb string")
            os.chdir(cwd0)

    # PW file format:
    #  a)  pw
    #      pw
    #      pw
    #  b)  filename1 <:::> pw1
    #      filename2 <:::> pw2

    if not pw_file:
        return None

    logger.debug(whoami() + "reading passwords in " + pw_file)

    try:
        with open(pw_file, "r") as f0:
            pw_list = f0.readlines()
    except Exception as e:
        logger.warning(whoami() + str(e) + ": cannot open/read pw file")
        return None

    cwd0 = os.getcwd()
    os.chdir(directory)

    pwlist = [pw.rstrip("\n") for pw in pw_list]
    PW = None

    # first try with <:::> if exists
    logger.info(whoami() + "trying specified password entries <:::> ...")
    for pw in pwlist:
        if "<:::>" not in pw:
            continue
        fn0 = pw.split("<:::>")[0].lstrip(" ").rstrip(" ")
        fn0 = fn0.split(".nzb")[0]
        pw0 = pw.split("<:::>")[1].lstrip(" ").rstrip(" ")
        pw0 = pw0.split(".nzb")[0]
        if fn0 != nzbname:
            continue
        logger.debug(whoami() + "Trying with entry: " + fn0 + " / " + pw0 + " for NZB " + nzbname)
        if test_password(pw0, rarname):
            PW = pw0
            logger.info(whoami() + "Found PW for NZB " + nzbname + ": " + PW)
            break
    if PW:
        os.chdir(cwd0)
        return PW

    # try
    logger.info(whoami() + "trying free password file entries ...")
    for pw in pwlist:
        if "<:::>" in pw:
            continue
        if test_password(pw, rarname):
            PW = pw
            logger.info(whoami() + "Found PW for NZB " + nzbname + ": " + PW)
            break

    os.chdir(cwd0)
    return PW


if __name__ == "__main__":
    rar = "/home/stephan/.ginzibix/incomplete/Pokemon.-.Der.Film.Du.bist.dran.2017.MULTi.COMPLETE.BLURAY{{4nUTnLgRJgtLXnv}}/_renamed0/P-DFDb15636597614VtJh190720Sha.part001.rar"
    pw = "4nUTnLgRJgtLXnv"
    test_password(pw, rar)


    '''logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # test if password protected
    directory = "/home/stephan/.ginzibix/incomplete/Captain.Underpants.Der.supertolle.erste.Film.2017.German.DTS.DL.720p.BluR{{HoU_uv68FZdz3s}}/_verifiedrars0"
    ipw = is_rar_password_protected(directory, logger)
    if ipw == 1:
        print("is pw protected!")
    elif ipw == -1:
        print("is NOT pw protected!")
    elif ipw == 0:
        print("is not a rar file!")

    print(ipw)

    if ipw != 1:
        sys.exit()
    
    # try passwords
    directory = "/home/stephan/.ginzibix/incomplete/therainS01E01/_verifiedrars0"
    pw_file = "/home/stephan/.ginzibix/PW_2"
    nzbname0 = "therainS01E01.nzb"
    pw = get_password(directory, pw_file, nzbname0, logger)
    print("Password: " + str(pw))

    # test if password protected
    directory = "/home/stephan/.ginzibix/incomplete/therainS01E01/_unpack0"
    rarname0 = "0OriJpkzUSAYmK.part1.rar"
    ipw = is_rar_password_protected(directory, rarname0, logger)
    if ipw == 1:
        print(rarname0 + " is pw protected!")
    elif ipw == -1:
        print(rarname0 + " is NOT pw protected!")
    elif ipw == 0:
        print(rarname0 + " is not a rar file!")

    directory = "/home/stephan/.ginzibix/incomplete/Der.Gloeckner.von.Notre/_renamed0"
    rarname0 = "Walt.Disneys.Der.Gloeckner.von.Notre.Dame.2.German.2000.DVDRIP.XviD-AIO.part01.rar"
    ipw = is_rar_password_protected(directory, rarname0, logger)
    if ipw == 1:
        print(rarname0 + " is pw protected!")
    elif ipw == -1:
        print(rarname0 + " is NOT pw protected!")
    elif ipw == 0:
        print(rarname0 + " is not a rar file!")'''
