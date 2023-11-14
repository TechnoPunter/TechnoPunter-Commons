import os

import yaml

RESOURCE_PATH_LOC = os.environ.get('RESOURCE_PATH')
CONF_PATH_LOC = os.path.join(RESOURCE_PATH_LOC, 'config.yaml')
GENERATED_PATH_LOC = os.environ.get('GENERATED_PATH')


def read_config(file_path: str = CONF_PATH_LOC):
    with open(file_path, 'r') as file:
        parent_config = yaml.safe_load(file)

    file_directory = os.path.dirname(file_path)

    # Get the list of included YAML filenames from the parent config
    child_file_paths = parent_config.get('include', [])

    merged_config = {}
    for child_file_path in child_file_paths:
        try:
            with open(file_directory + "/" + child_file_path + "-local.yaml", 'r') as child_file:
                child_config = yaml.safe_load(child_file)
        except FileNotFoundError:
            with open(file_directory + "/" + child_file_path + ".yaml", 'r') as child_file:
                child_config = yaml.safe_load(child_file)

        # Merge the child_config into the merged_config using update()
        merged_config.update(child_config)

    parent_config.pop('include', [])
    parent_config.update(merged_config)
    parent_config['generated'] = GENERATED_PATH_LOC

    return parent_config


cfg = read_config()

# Example usage:
if __name__ == '__main__':
    config = read_config()
    print(yaml.dump(config))
