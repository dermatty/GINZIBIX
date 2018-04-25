from lxml import etree
import sys
import time

nzbfile = "/home/stephan/.ginzibix/nzb_bak/Der.Gloeckner.von.Notre.nzb"
tree = etree.parse(nzbfile)

root = tree.getroot()

try:
    tree = etree.parse(nzbfile)
except Exception as e:
    print(str(e))
    sys.exit()
nzbroot = tree.getroot()
filedic = {}
bytescount0 = 0
for r in nzbroot:
    headers = r.attrib
    try:
        date = headers["date"]
        age = int((time.time() - float(date))/(24 * 3600))
        subject = headers["subject"]
        hn_list = subject.split('"')
        hn = hn_list[1]
    except Exception as e:
        continue
    for s in r:
        filelist = []
        segfound = True
        for i, r0 in enumerate(s):
            if r0.tag[-5:] == "group":
                segfound = False
                continue
            nr0 = r0.attrib["number"]
            filename = "<" + r0.text + ">"
            bytescount = int(r0.attrib["bytes"])
            bytescount0 += bytescount
            filelist.append((filename, int(nr0), bytescount))
        i -= 1
        if segfound:
            filelist.insert(0, (age, int(nr0)))
            filedic[hn] = filelist

for key, item in filedic.items():
    print(key)
