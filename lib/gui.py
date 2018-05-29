import psutil
import time
import sys
import datetime


class GUI_Drawer:

    def draw(self, data, pwdb_msg, server_config, threads):
        if not data or not pwdb_msg or not server_config or not threads:
            return
        bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
        avgmiblist = avgmiblist00
        max_mem_needed = 0
        bytescount0 = bytescount00
        bytescount0 += 0.00001
        overall_size += 0.00001
        availmem0 = availmem00
        # get Mib downloaded
        if len(avgmiblist) > 50:
            del avgmiblist[0]
        if len(avgmiblist) > 10:
            avgmib_dic = {}
            for (server_name, _, _, _, _, _, _, _, _) in server_config:
                bytescountlist = [bytescount for (_, bytescount, download_server0) in avgmiblist if server_name == download_server0]
                if len(bytescountlist) > 2:
                    avgmib_db = sum(bytescountlist)
                    avgmib_mint = min([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    avgmib_maxt = max([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    # print(avgmib_maxt, avgmib_mint)
                    avgmib_dic[server_name] = (avgmib_db / (avgmib_maxt - avgmib_mint)) / (1024 * 1024) * 8
                else:
                    avgmib_dic[server_name] = 0
            for server_name, avgmib in avgmib_dic.items():
                mem_needed = ((psutil.virtual_memory()[0] - psutil.virtual_memory()[1]) - availmem0) / (1024 * 1024 * 1024)
                if mem_needed > max_mem_needed:
                    max_mem_needed = mem_needed
        # set in all threads servers alive timestamps
        # check if server is longer off > 120 sec, if yes, kill thread & stop server
        if nzbname:
            print("--> Downloading " + nzbname)
        else:
            print("--> Downloading paused!" + " " * 30)
        try:
            print("MBit/sec.: " + str([sn + ": " + str(int(av)) + "  " for sn, av in avgmib_dic.items()]) + " max. mem_needed: "
                  + "{0:.3f}".format(max_mem_needed) + " GB                 ")
        except UnboundLocalError:
            print("MBit/sec.: --- max. mem_needed: " + str(max_mem_needed) + " GB                ")
        gbdown0 = 0
        mbitsec0 = 0
        for t_bytesdownloaded, t_last_timestamp, t_idn in threads:
            # for k, (t, last_timestamp) in enumerate(threads):
            gbdown = t_bytesdownloaded / (1024 * 1024 * 1024)
            gbdown0 += gbdown
            gbdown_str = "{0:.3f}".format(gbdown)
            mbitsec = (t_bytesdownloaded / (time.time() - t_last_timestamp)) / (1024 * 1024) * 8
            mbitsec0 += mbitsec
            mbitsec_str = "{0:.1f}".format(mbitsec)
            print(t_idn + ": Total - " + gbdown_str + " GB" + " | MBit/sec. - " + mbitsec_str + "                        ")
        print("-" * 60)
        gbdown0 += already_downloaded_size
        gbdown0_str = "{0:.3f}".format(gbdown0)
        perc0 = gbdown0 / overall_size
        if perc0 > 1:
            perc00 = 1
        else:
            perc00 = perc0
        print(gbdown0_str + " GiB (" + "{0:.1f}".format(perc00 * 100) + "%) of total "
              + "{0:.2f}".format(overall_size) + " GiB | MBit/sec. - " + "{0:.1f}".format(mbitsec0) + " " * 10)
        for key, item in filetypecounter00.items():
            print(key + ": " + str(item["counter"]) + "/" + str(item["max"]) + ", ", end="")
        if nzbname:
            trailing_spaces = " " * 10
        else:
            trailing_spaces = " " * 70
        print("Health: {0:.4f}".format(article_health * 100) + "%" + trailing_spaces)
        if mbitsec0 > 0 and perc0 < 1:
            eta = ((overall_size - gbdown0) * 1024)/(mbitsec0 / 8)
            print("Eta: " + str(datetime.timedelta(seconds=int(eta))) + " " * 30)
        else:
            print("Eta: - (done!)" + " " * 40)
        print()
        msg0 = pwdb_msg
        if msg0:
            print(" " * 120)
            sys.stdout.write("\033[F")
            print(msg0[-1][:120])
        for _ in range(len(threads) + 8):
            sys.stdout.write("\033[F")
