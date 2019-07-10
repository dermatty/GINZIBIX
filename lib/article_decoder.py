import re
import ginzyenc
import os
import time
import queue
import signal
from .aux import PWDBSender
from threading import Thread
from .mplogging import setup_logger, whoami
from setproctitle import setproctitle


TERMINATED = False
MAX_THREADS = 4
IS_IDLE = False


def decoder_is_idle():
    return IS_IDLE


def decoder_set_idle(ie):
    global IS_IDLE
    IS_IDLE = ie


class SigHandler_Decoder:
    def __init__(self, logger):
        self.logger = logger

    def sighandler(self, a, b):
        global TERMINATED
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


def decode_articles(mp_work_queue0, mp_loggerqueue, filewrite_lock):
    setproctitle("gzbx." + os.path.basename(__file__))
    logger = setup_logger(mp_loggerqueue, __file__)
    logger.info(whoami() + "starting article decoder process")

    sh = SigHandler_Decoder(logger)
    signal.signal(signal.SIGINT, sh.sighandler)
    signal.signal(signal.SIGTERM, sh.sighandler)

    pwdb = PWDBSender()

    bytesfinal = bytearray()
    while not TERMINATED:
        res0 = None
        decoder_set_idle(True)
        while not TERMINATED:
            try:
                res0 = mp_work_queue0.get_nowait()
                break
            except (queue.Empty, EOFError):
                pass
            except Exception as e:
                logger.warning(whoami() + str(e))
            time.sleep(0.1)
        if not res0 or TERMINATED:
            logger.info(whoami() + "exiting decoder process!")
            break
        decoder_set_idle(False)

        infolist, save_dir, filename, filetype = res0
        logger.debug(whoami() + "starting decoding for " + filename)
        # del bytes0
        bytesfinal = bytearray()
        status = 0   # 1: ok, 0: wrong yenc structure, -1: no crc32, -2: crc32 checksum error, -3: decoding error
        statusmsg = "ok"

        # ginzyenc
        full_filename = save_dir + filename
        i = 0
        for info in infolist:
            try:
                lastline = info[-1].decode("latin-1")
                m = re.search('size=(.\d+?) ', lastline)
                if m:
                    size = int(m.group(1))
            except Exception as e:
                logger.warning(whoami() + str(e) + ", guestimate size ...")
                size = int(sum(len(i) for i in info.lines) * 1.1)
            try:
                decoded_data, output_filename, crc, crc_yenc, crc_correct = ginzyenc.decode_usenet_chunks(info, size)
                #if filename.endswith("part01.rar") and i == 3:
                #    pass
                #else:
                bytesfinal.extend(decoded_data)
            except Exception as e:
                logger.warning(whoami() + str(e) + ": cannot perform ginzyenc")
                status = -3
                statusmsg = "ginzyenc decoding error!"
                # continue decoding, maybe it can be repaired ...?
            i += 1
        logger.debug(whoami() + "decoding for " + filename + ": success!")
        try:
            with filewrite_lock:
                if not os.path.isdir(save_dir):
                    os.makedirs(save_dir)
                with open(full_filename, "wb") as f0:
                    f0.write(bytesfinal)
                    f0.flush()
                    f0.close()
        except Exception as e:
            statusmsg = "file_error"
            logger.error(whoami() + str(e) + " in file " + filename)
            status = -4
        logger.info(whoami() + filename + " decoded with status " + str(status) + " / " + statusmsg)
        pwdbstatus = 2
        if status in [-3, -4]:
            pwdbstatus = -1
        try:
            # pwdb.db_file_update_status(filename, pwdbstatus)
            pwdb.exc("db_file_update_status", [filename, pwdbstatus], {})
            logger.debug(whoami() + "updated DB for " + filename + ", db.status=" + str(pwdbstatus))
        except Exception as e:
            logger.error(whoami() + str(e) + ": cannot update DB for " + filename)
        i += 1
    logger.debug(whoami() + "exited!")


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
