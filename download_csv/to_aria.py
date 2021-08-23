import os
if not os.path.exists("output"):
    os.mkdir("output")

filelist = open("download_urls.txt", "r").read().split("\n")
with open("for_aria.txt", "w") as f:
    for fil in filelist:
        if fil == "":
            continue
        output_dir = "output" + "/" + "/".join(fil.split("/")[4:][:-1])

        f.write(fil+"\n")
        f.write(" dir="+output_dir+"\n")
        f.write(" continue=true\n")
        f.write(" max-connection-per-server=16\n")
        f.write(" split=16\n")
        f.write(" min-split-size=20M\n\n")
