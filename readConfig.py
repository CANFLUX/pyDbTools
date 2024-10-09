# Read YAML configuration file(s) for python based tasks
# Intended to be called by other scripts, not called by user directly
# Written by June Skeeter

import os
import sys
import yaml # Note: you need to install the "pyyaml" package, e.g., pip install pyyaml
import argparse

def set_user_configuration(pathDeffs = 'config_files/user_path_definitions.yml',tasks={}):
    # get current crectory
    wd = os.getcwd()
    # temporarily set directory
    os.chdir(os.path.split(__file__)[0])
    pathDeffs = os.path.abspath(pathDeffs)
    print(wd)
    # Parse the config settings
    with open('config_files/config.yml') as yml:
        config = yaml.safe_load(yml)
        if os.path.isfile(pathDeffs):
            with open(pathDeffs) as yml:
                config.update(yaml.safe_load(yml))
        else:
            print(f"WARNING: missing {pathDeffs}")
            print("This will cause issues, please create your own path definition file")

    # Import the user specified configurations (exit if they don't exist)
    if tasks != {}:
        for key,value in tasks.items():
            config[key] = {}
            if isinstance(value,str):value=[value]
            for req in value:
                req = os.path.abspath(req)
                if os.path.isfile(req):
                    with open(req) as yml:
                        config[key].update(yaml.safe_load(yml))
                else:
                    sys.exit(f"Missing {req}")
    os.chdir(wd)
    return(config)

# If called from command line ...
if __name__ == '__main__':
    
    CLI=argparse.ArgumentParser()

    CLI.add_argument(
        "--pathDeffs",
        nargs = '?',
        type = str,
        default = '../user_path_definitions.yml'
    )

    CLI.add_argument(
        "--tasks", 
        nargs='+',
        type=str,
        default=[],
        )

    # Parse the args and make the call
    args = CLI.parse_args()

    args.tasks = {k:v for k,v in zip(args.tasks[0::2],args.tasks[1::2])}
    set_user_configuration(args.pathDeffs,args.tasks)