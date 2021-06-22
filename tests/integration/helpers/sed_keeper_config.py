import xml.etree.ElementTree as ET
import logging
import os
import subprocess


HELPERS_DIR = os.path.dirname(__file__)

def dump_file(file, tmp_file):
    # local("cp %s %s" % (file, tmp_file))
    cmd = "cp -rf {} {}".format(file, tmp_file)
    subprocess.check_call(cmd, shell=True)

def remove_file(file):
    cmd = "rm -rf {}".format(file)
    subprocess.check_call(cmd, shell=True)

    
def get_tmp_file_name(file, suffix=".tmp"):
    tmp_file_name = os.path.basename(file).split('.')[0] + suffix
    return os.path.join(os.path.dirname(file), tmp_file_name)

def sed_xml_obj(file, obi_kye, obj_val):
    tree = ET.parse(file)
    root = tree.getroot()
    # print("list0")
    for object in root.findall(obi_kye):
        deleted = str(object.text)
        if (deleted in [obj_val]):
            root.remove(object)
    tree.write(file)

def get_zookeeper_path(file_name):
    return os.path.join(HELPERS_DIR, "{}".format(file_name))

def sed_zookeeper_listen():
    keeper_confs = [
        get_zookeeper_path(i) for i in ["keeper_config1.xml", "keeper_config2.xml", "keeper_config3.xml"]]
    for keeper_conf in keeper_confs:
        keeper_tmp_file = get_tmp_file_name(keeper_conf)
        logging.info("zookeeper tmp config file name {}".format(keeper_tmp_file))
        # print("zookeeper tmp config file name {}".format(keeper_tmp_file))
        dump_file(keeper_conf, keeper_tmp_file)
        sed_xml_obj(keeper_conf, "listen_host", "::")

def recovery_zookeeper_listen(suffix=".tmp"):
    keeper_confs = [
        get_zookeeper_path(i) for i in ["keeper_config1.xml", "keeper_config2.xml", "keeper_config3.xml"]]
    for keeper_conf in keeper_confs:
        tmp_file = get_tmp_file_name(keeper_conf)
        if os.path.exists(tmp_file):
            dump_file(tmp_file, keeper_conf)
            remove_file(tmp_file)

# sed_zookeeper_listen()
# recovery_zookeeper_listen()
 