import configparser

def setConf():
    config_url="./config/config.conf"
    conf = configparser.ConfigParser()
    conf.read(config_url)
    return conf

def setVariable():
    config_url="./config/variable.ini"
    conf = configparser.ConfigParser()
    conf.read(config_url)
    return conf

conf = setConf()
varible = setVariable()