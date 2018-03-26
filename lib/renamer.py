import os
import time
import shutil
import glob
import par2lib
from random import randint


def renamer(source_dir, dest_dir, logger):
    if source_dir[-1] != "/":
        source_dir += "/"
    if dest_dir[-1] != "/":
        dest_dir += "/"
    cwd0 = os.getcwd()

    # logger.debug("Starting renamer ...")
    os.chdir(source_dir)

    p2obj = None
    p2basename = None

    while True:

        # get all files not yet .renamed
        print("1. Reading _downloaded0")
        notrenamedfiles = []
        for fn in glob.glob("*"):
            fn0 = fn.split("/")[-1]
            if not fn0.endswith(".renamed"):
                notrenamedfiles.append((fn0, par2lib.calc_file_md5hash_16k(fn0)))
        # todo:
        #     inqueue.get_nowait and quit if downloader finished
        if not notrenamedfiles:
            break
            continue
        print(notrenamedfiles)
        print("-" * 50)

        # get all files in renamed
        print("2. Reading _renamed0")
        renamedfiles = []
        for fn in glob.glob(dest_dir + "*"):
            fn0 = fn.split("/")[-1]
            if fn0.endswith(".par2") and not p2obj:
                p2obj = par2lib.Par2File(fn)
                p2basename = fn.split(".par2")[0]
                # todo: test par2
            renamedfiles.append(fn0)

        # if par2 not in _renamed search main par2 in _downloaded0
        if not p2obj:
            print("No p2obj yet found, looking in _downloaded0")
            mainparlist = []
            for fn, _ in notrenamedfiles:
                p2_test = par2lib.Par2File(fn)
                if p2_test.filenames():
                    filelen = len(p2_test.contents)
                    mainparlist.append((fn, filelen))
            if mainparlist:
                p2min = sorted(mainparlist, key=lambda length: length[1])[0][0]
                # todo: teste par2 file!!!
                p2obj = par2lib.Par2File(p2min)
                p2basename = p2min.split(".par2")[0]
                print("p2obj found: " + str(p2min))

        # search for not yet renamed par2/vol files
        not_renamed_par2list = []
        for pname, phash in notrenamedfiles:
            with open(pname, "rb") as f:
                content = f.read()
                # check if PAR 2.0\0Creator\0 in last 50 bytes
                # if not -> it's no par file
                lastcontent = content[-50:]
                bstr0 = b"PAR 2.0\0Creator\0"
                if bstr0 not in lastcontent:
                    continue
                # check for PAR2\x00 in first 50 bytes
                # if no -> it's par2vol, else: could be par2
                firstcontent = content[:100]
                bstr0 = b"PAR2\x00"  # PAR2\x00
                bstr1 = b"PAR 2.0\x00FileDesc"  # PAR 2.0\x00FileDesc'
                if bstr0 not in firstcontent:
                    ptype = "par2vol"
                elif bstr1 in firstcontent:
                    ptype = "par2"
                    p2obj = par2lib.Par2File(pname)
                    p2basename = pname.split(".par2")[0]
                else:
                    ptype = "par2vol"
                not_renamed_par2list.append((pname, ptype, phash))
        # print(not_renamed_par2list)
        if not_renamed_par2list:
            for pname, ptype, phash in not_renamed_par2list:
                pp = (pname, phash)
                if ptype == "par2":
                    shutil.copyfile(source_dir + pname, dest_dir + pname)
                    os.rename(source_dir + pname, source_dir + pname + ".renamed")
                    notrenamedfiles.remove(pp)
                elif ptype == "par2vol" and p2basename:
                    # todo: if not p2basename ??
                    volpart1 = randint(1, 99)
                    volpart2 = randint(1, 99)
                    shutil.copyfile(source_dir + pname, dest_dir + p2basename + ".vol" + str(volpart1).zfill(3) +
                                    "+" + str(volpart2).zfill(3) + ".PAR2")
                    os.rename(source_dir + pname, source_dir + pname + ".renamed")
                    notrenamedfiles.remove(pp)

        # rename + move rar files
        if p2obj:
            rarfileslist = p2obj.md5_16khash()
            notrenamedfiles0 = notrenamedfiles[:]
            for a_name, a_md5 in notrenamedfiles0:
                pp = (a_name, a_md5)
                try:
                    r_name = [fn for fn, r_md5 in rarfileslist if r_md5 == a_md5][0]
                    if r_name != a_name:
                        shutil.copyfile(source_dir + a_name, dest_dir + r_name)
                    else:
                        shutil.copyfile(source_dir + a_name, dest_dir + a_name)
                    os.rename(source_dir + a_name, source_dir + a_name + ".renamed")
                    notrenamedfiles.remove(pp)
                except IndexError:
                    pass
                except Exception as e:
                    print(str(e))

        # todo: copy all other files
        # .
        # .
        # .
        for a_name, a_md5 in notrenamedfiles:
            shutil.copyfile(source_dir + a_name, dest_dir + a_name)
            os.rename(source_dir + a_name, source_dir + a_name + ".renamed")
        time.sleep(2)
    os.chdir(cwd0)


renamer("/home/stephan/.ginzibix/incomplete/Florida_project/_downloaded0",
        "/home/stephan/.ginzibix/incomplete/Florida_project/_renamed0", None)
