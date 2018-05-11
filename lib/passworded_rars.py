import os
import subprocess
import re
import logging
import logging.handlers
import sys
import glob

lpref = __name__ + " - "


def get_sorted_rar_list(directory):
    rarlist = []
    for rarf in glob.glob(directory + "*.rar"):
        gg = re.search(r"[0-9]+[.]rar", rarf, flags=re.IGNORECASE)
        rarlist.append((int(gg.group().split(".")[0]), rarf))
    rar_sortedlist = []
    if rarlist:
        rar_sortedlist = sorted(rarlist, key=lambda nr: nr[0])
    return rar_sortedlist


def is_rar_password_protected(directory, logger):
    # return value:
    #    1 ... is pw protected
    #    0 ... is not a rar file
    #    -1 .. is not pw protected
    #    -2 .. no rars in dir!
    cwd0 = os.getcwd()
    if directory[-1] != "/":
        directory += "/"
    rars = get_sorted_rar_list(directory)
    if not rars:
        return -2
    rarname0 = rars[0][1]
    rarname = rarname0.split("/")[-1]
    os.chdir(directory)
    logger.info(lpref + "checking if rar is passworded")
    gg = re.search(r"[.]rar$", rarname)
    if not gg:
        logger.info(lpref + rarname + "is not a rar file!")
        return 0
    ssh = subprocess.Popen(["unrar", "t", "-p-", rarname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
    ssherr = ssh.stderr.readlines()
    is_pw_protected = False
    for ss in ssherr:
        ss0 = ss.decode("utf-8")
        if "Corrupt file or wrong password" in ss0:
            logger.info(rarname + " is password protected")
            is_pw_protected = True
            break
    os.chdir(cwd0)
    if is_pw_protected:
        return 1
    else:
        return -1


def get_password(directory, pw_file, nzbname0, logger):
    if directory[-1] != "/":
        directory += "/"
    rars = get_sorted_rar_list(directory)
    if not rars:
        return None
    rarname0 = rars[0][1]
    rarname = rarname0.split("/")[-1]
    nzbname = nzbname0.split(".nzb")[0]
    # PW file format:
    #  a)  pw
    #      pw
    #      pw
    #  b)  filename1 <:::> pw1
    #      filename2 <:::> pw2
    try:
        with open(pw_file, "r") as f0:
            pw_list = f0.readlines()
    except Exception as e:
        logger.warning(str(e) + ": cannot open/read pw file")
        return None

    cwd0 = os.getcwd()
    os.chdir(directory)

    pwlist = [pw.rstrip("\n") for pw in pw_list]
    PW = None

    # first try with <:::> if exists
    logger.info(lpref + "trying specified password entries <:::> ...")
    for pw in pwlist:
        if "<:::>" not in pw:
            continue
        fn0 = pw.split("<:::>")[0].lstrip(" ").rstrip(" ")
        fn0 = fn0.split(".nzb")[0]
        pw0 = pw.split("<:::>")[1].lstrip(" ").rstrip(" ")
        pw0 = pw0.split(".nzb")[0]
        if fn0 != nzbname:
            continue
        logger.debug(lpref + "Trying with entry: " + fn0 + " / " + pw0 + " for NZB " + nzbname)
        ssh = subprocess.Popen(["unrar", "t", "-p" + pw0, rarname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
        ssherr = ssh.stderr.readlines()
        if not ssherr:
            PW = pw0
            logger.info(lpref + "Found PW for NZB " + nzbname + ": " + PW)
            break
    if PW:
        os.chdir(cwd0)
        return PW

    # try
    logger.info(lpref + "trying free password file entries ...")
    for pw in pwlist:
        if "<:::>" in pw:
            continue
        ssh = subprocess.Popen(["unrar", "t", "-p" + pw, rarname], shell=False, stdout=subprocess.PIPE, stderr=subprocess. PIPE)
        ssherr = ssh.stderr.readlines()
        if not ssherr:
            PW = pw
            logger.info(lpref + "Found PW for NZB " + nzbname + ": " + PW)
            break

    os.chdir(cwd0)
    return PW


if __name__ == "__main__":
    logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # test if password protected
    directory = "/home/stephan/.ginzibix/incomplete/therainS01E01/_unpack0"
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
    '''directory = "/home/stephan/.ginzibix/incomplete/therainS01E01/_unpack0"
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
