import yaml
import os

setup = os.path.dirname(os.path.realpath(__file__))
base = os.path.split(setup)[0]

with open(f'{setup}/user_path_definitions_template.yml') as yml:
    config = yaml.safe_load(yml)

print('Instructions: verify paths or overwrite with new values, :')
for v in ['EddyPro']:
    print(f'{v}: {config["rootDir"][v]}')
    i = input("Input new value to overwrite, skip to accept defalut: ")
    if i !='':
        config["rootDir"][v] = os.path.abspath(i)

config['rootDir']['highfreq'] = os.path.abspath(f'{base}/highfreq')
config['BiometUser']['Biomet.net'] = os.path.abspath(f'{base}/Biomet.net')
config['BiometUser']['Database'] = os.path.abspath(f'{base}/Database')
with open(f'{base}/user_path_definitions.yml', 'w') as outfile:
    yaml.dump(config, outfile, default_flow_style=False)