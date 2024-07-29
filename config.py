import yaml

CONFIG_PATH = "config.yaml"

def loadConfig():
    with open(CONFIG_PATH, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise Exception(f"Error loading config file: {exc}")