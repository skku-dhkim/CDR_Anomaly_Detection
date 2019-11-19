import os
import config.file_path as fp
import utils.marker as mk
import glob

running_files = glob.glob(fp.run_dir() + "*.run")
for f in running_files:
    with open(f, "r") as file:
        pid = file.readline()
    os.system('kill {}'.format(pid))
    mk.debug_info("Process '{}' is successfully terminated.".format(pid))
