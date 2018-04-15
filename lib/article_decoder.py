import re
import yenc
import os

lpref = __name__ + " - "

def decode_articles(mp_work_queue0, mp_result_queue0, pwdb, logger):
    logger.info(lpref + "Started decoder process")
    bytes0 = bytearray()
    bytesfinal = bytearray()
    while True:
        try:
            res0 = mp_work_queue0.get()
        except KeyboardInterrupt:
            return
        if not res0:
            logger.info(lpref + "Exiting decoder process!")
            break
        infolist, save_dir, filename, filetype = res0
        del bytes0
        bytesfinal = bytearray()
        status = 0   # 1: ok, 0: wrong yenc structure, -1: no crc32, -2: crc32 checksum error, -3: decoding error
        statusmsg = "ok"
        for info in infolist:
            headerok = False
            trailerok = False
            partfound = False
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
                logger.warning(lpref + filename + ": wrong yenc structure detected")
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
            logger.info(lpref + filename + " decoded and saved!")
            # calc hash for rars
            if filetype == "rar":
                md5 = 0  # calc_file_md5hash(save_dir + filename)
                if md5 == -1:
                    raise("Cannot calculate md5 hash")
                # logger.info(full_filename + " md5: " + str(md5))
        except Exception as e:
            statusmsg = "file_error"
            logger.error(lpref + str(e) + ": " + filename)
            status = -4
        logger.info(lpref + filename + " decoded with status " + str(status) + " / " + statusmsg)
        pwdbstatus = 2
        if status in [-3, -4]:
            pwdbstatus = -1
        try:
            pwdb.db_file_update_status(filename, pwdbstatus)
            logger.info(lpref + "Updated DB for " + filename + ", db.status=" + str(pwdbstatus))
            # s0 = pwdb.db_file_getstatus(filename)
            # logger.info("RETRIEVED " + filename + " status:" + str(s0))
        except Exception as e:
            logger.error(lpref + str(e) + ": cannot update DB for " + filename)
        mp_result_queue0.put((filename, full_filename, filetype, status, statusmsg, md5))
