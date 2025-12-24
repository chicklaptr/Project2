from configparser import ConfigParser
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(BASE_DIR, "config", "database.ini")
def load_config(section='postgresql'):
    parser = ConfigParser()
    parser.read(CONFIG_FILE)
    print("CONFIG_FILE =", CONFIG_FILE)
    print("Files loaded =", parser.read(CONFIG_FILE))
    print("Sections =", parser.sections())


    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {CONFIG_FILE} file")

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)
