import re
import yenc
import os
import time
import queue
import signal

lpref = __name__.split("lib.")[-1] + " - "

TERMINATED = False


class SigHandler_Decoder:
    def __init__(self, logger):
        self.logger = logger

    def sighandler(self, a, b):
        global TERMINATED
        self.logger.info(lpref + "terminating ...")
        TERMINATED = True


def decode_articles(mp_work_queue0, pwdb, logger):
    sh = SigHandler_Decoder(logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    logger.info(lpref + "starting decoder process")
    bytes0 = bytearray()
    bytesfinal = bytearray()
    while not TERMINATED:
        res0 = None
        while not TERMINATED:
            try:
                res0 = mp_work_queue0.get_nowait()
                break
            except (queue.Empty, EOFError):
                pass
            except Exception as e:
                logger.warning(lpref + str(e))
            time.sleep(0.1)
        if not res0 or TERMINATED:
            logger.info(lpref + "exiting decoder process!")
            break

        infolist, save_dir, filename, filetype = res0
        del bytes0
        bytesfinal = bytearray()
        status = 0   # 1: ok, 0: wrong yenc structure, -1: no crc32, -2: crc32 checksum error, -3: decoding error
        statusmsg = "ok"
        for info in infolist:
            headerok = False
            trailerok = False
            trail_crc = None
            head_crc = None
            bytes0 = bytearray()
            partnr = 0
            artsize0 = 0
            for inf in info:
                try:
                    inf0 = inf.decode()
                    if inf0 == "":
                        continue
                    if inf0.startswith("=ybegin"):
                        try:
                            artname = re.search(r"name=(\S+)", inf0).group(1)
                            artsize = int(re.search(r"size=(\S+)", inf0).group(1))
                            artsize0 += artsize
                            m_obj = re.search(r"crc32=(\S+)", inf0)
                            if m_obj:
                                head_crc = m_obj.group(1)
                            headerok = True
                        except Exception as e:
                            logger.warning(lpref + str(e) + ": malformed =ybegin header in article " + artname)
                        continue
                    if inf0.startswith("=ypart"):
                        partnr += 1
                        continue
                    if inf0.startswith("=yend"):
                        try:
                            artsize = int(re.search(r"size=(\S+)", inf0).group(1))
                            m_obj = re.search(r"crc32=(\S+)", inf0)
                            if m_obj:
                                trail_crc = m_obj.group(1)
                            trailerok = True
                        except Exception as e:
                            logger.warning(lpref + str(e) + ": malformed =yend trailer in article " + artname)
                        continue
                except Exception as e:
                    pass
                bytes00 = bytes0
                try:
                    bytes0.extend(inf)
                    pass
                except KeyboardInterrupt:
                    return
                except Exception as e:
                    logger.warning(lpref + str(e) + ": " + filename)
                    bytes0 = bytes00
            if not headerok or not trailerok:  # or not partfound or partnr > 1:
                logger.warning(lpref + ": wrong yenc structure detected in file " + filename)
                statusmsg = "yenc_structure_error"
                status = 0
            _, decodedcrc32, decoded = yenc.decode(bytes0)
            if not head_crc and not trail_crc:
                statusmsg = "no_pcrc32_error"
                logger.warning(lpref + filename + ": no pcrc32 detected")
                status = -1
            else:
                try:
                    head_crc0 = "" if not head_crc else head_crc.lower()
                    trail_crc0 = "" if not trail_crc else trail_crc.lower()
                    crc32list = [head_crc0.strip("0"), trail_crc0.strip("0")]
                    crc32 = decodedcrc32.lower().strip("0")
                except Exception as e:
                    logger.error(str(e))
                if crc32 not in crc32list:
                    # logger.warning(filename + ": CRC32 checksum error: " + crc32 + " / " + str(crc32list))
                    statusmsg = "crc32checksum_error: " + crc32 + " / " + str(crc32list)
                    status = -2
            bytesfinal.extend(decoded)
        if artsize0 != len(bytesfinal):
            statusmsg = "article file length wrong"
            status = -3
            logger.info(lpref + "Wrong article length: should be " + str(artsize0) + ", actually was " + str(len(bytesfinal)))
        md5 = None
        full_filename = save_dir + filename
        try:
            if not os.path.isdir(save_dir):
                os.makedirs(save_dir)
            with open(full_filename, "wb") as f0:
                f0.write(bytesfinal)
                f0.flush()
                f0.close()
            # calc hash for rars
            if filetype == "rar":
                md5 = 0  # calc_file_md5hash(save_dir + filename)
                if md5 == -1:
                    raise("Cannot calculate md5 hash")
                # logger.info(full_filename + " md5: " + str(md5))
        except Exception as e:
            statusmsg = "file_error"
            logger.error(lpref + str(e) + " in file " + filename)
            status = -4
        logger.info(lpref + filename + " decoded with status " + str(status) + " / " + statusmsg)
        pwdbstatus = 2
        if status in [-3, -4]:
            pwdbstatus = -1
        try:
            pwdb.db_file_update_status(filename, pwdbstatus)
            logger.debug(lpref + "updated DB for " + filename + ", db.status=" + str(pwdbstatus))
        except Exception as e:
            logger.error(lpref + str(e) + ": cannot update DB for " + filename)
    logger.info(lpref + "terminated!")


# ---- test only ----
def decode_articles_standalone(infolist):
    bytes0 = bytearray()
    bytesfinal = bytearray()
    headerok = False
    trailerok = False
    trail_crc = None
    head_crc = None
    partnr = 0
    artsize0 = 0
    for inf in infolist:
        try:
            inf0 = inf.decode()
            if inf0 == "":
                continue
            if inf0.startswith("=ybegin"):
                try:
                    artname = re.search(r"name=(\S+)", inf0).group(1)
                    artsize = int(re.search(r"size=(\S+)", inf0).group(1))
                    artsize0 += artsize
                    m_obj = re.search(r"crc32=(\S+)", inf0)
                    if m_obj:
                        head_crc = m_obj.group(1)
                    headerok = True
                except Exception as e:
                    print(lpref + str(e) + ": malformed =ybegin header in article " + artname)
                continue
            if inf0.startswith("=ypart"):
                partnr += 1
                continue
            if inf0.startswith("=yend"):
                try:
                    artsize = int(re.search(r"size=(\S+)", inf0).group(1))
                    m_obj = re.search(r"crc32=(\S+)", inf0)
                    if m_obj:
                        trail_crc = m_obj.group(1)
                    trailerok = True
                except Exception as e:
                    print(lpref + str(e) + ": malformed =yend trailer in article " + artname)
                continue
        except Exception as e:
            pass
        bytes00 = bytes0
        try:
            bytes0.extend(inf)
            pass
        except KeyboardInterrupt:
            return
        except Exception as e:
            print(lpref + str(e))
            print(inf)
            return
            bytes0 = bytes00

    if not headerok or not trailerok:  # or not partfound or partnr > 1:
        print(lpref + ": wrong yenc structure detected")
    try:
        _, decodedcrc32, decoded = yenc.decode(bytes0)
    except:
        pass
    if not head_crc and not trail_crc:
        print(lpref + ": no pcrc32 detected")
    else:
        try:
            head_crc0 = "" if not head_crc else head_crc.lower()
            trail_crc0 = "" if not trail_crc else trail_crc.lower()
            crc32list = [head_crc0.strip("0"), trail_crc0.strip("0")]
            crc32 = decodedcrc32.lower().strip("0")
        except Exception as e:
            print(str(e))
        if crc32 not in crc32list:
            # logger.warning(filename + ": CRC32 checksum error: " + crc32 + " / " + str(crc32list))
            print("crc32checksum_error: " + crc32 + " / " + str(crc32list))
    bytesfinal.extend(decoded)
    if artsize0 != len(bytesfinal):
        print(lpref + "Wrong article length: should be " + str(artsize0) + ", actually was " + str(len(bytesfinal)))
    return bytesfinal


# 9E 92 93 9D 4A 93 9D 4A 8F 97 9A 9E A3 34

'''import sys
size, crc32, encoded = yenc.encode(b'data\r\n')
_, _, decoded = yenc.decode(encoded)
print(decoded)
sys.exit()


import string

yenc42 = "".join(map(lambda x: chr((x-42) & 255), range(256)))
yenc64 = "".join(map(lambda x: chr((x-64) & 255), range(256)))

data = [b"=ybegin line=128 size=14 name=ginzi.txt",
        b'\x9E\x92\x93\x9D\x4A\x93\x9D\x4A\x8F\x97\x9A\x9E\xA3\x34\x0D\x0A',
        b"=yend size=173 crc32=8828b45c\r\n"]


def conv(x):
    s = ""
    if x not in [13, 10]:
        s = chr(x)
    return s


s = "".join(map(conv, data[1]))


i = 1
buffer = []
line = data[i]
print(line)
print("-------------")
if line[-2:] == b"\r\n":
    line = line[:-2]
elif line[-1:] in b"\r\n":
    line = line[:-1]
a = s.translate(yenc42)
b = a.translate(yenc64)
print(b)

#buffer.append(data[0].translate(yenc42))
#for data in data[1:]:
#    data = data.translate(yenc42)
#    buffer.append(data[0].translate(yenc64))
#    buffer.append(data[1:])

#print(buffer)

#print(data0)

data = [b"=ybegin line=128 size=14 name=ginzi.txt",
        b'\x9E\x92\x93\x9D\x4A\x93\x9D\x4A\x8F\x97\x9A\x9E\xA3\x34\x0D\x0A',
        b"=yend size=14 crc32=e938d3a5\r\n"]

# dataenc = [d.encode() for d in data]
# print(dataenc)
# _, _, decoded = yenc.decode(data[1])
# print(decoded)
print(decode_articles_standalone(data))'''
