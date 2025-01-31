import yaml
import logging

def load_config(config_file='config/mvs_config.yaml'):
    """
    Load the configuration from the specified YAML file.
    
    Parameters:
    config_file (str): The path to the YAML configuration file.

    Returns:
    dict: The loaded configuration as a dictionary.
    """
    try:
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return None

def get_refresh_buffer(mv_name, config):
    """
    Get the refresh buffer for the specified MV from the configuration.
    
    Parameters:
    mv_name (str): The name of the materialized view.
    config (dict): The loaded configuration dictionary.

    Returns:
    int: The refresh buffer in minutes.
    """
    # Access the list of MVs
    mvs_list = config.get('mvs', [])
    
    # Iterate through the list to find the matching MV
    for mv in mvs_list:
        if mv.get('name') == mv_name:
            return mv.get('refresh_buffer_minutes', 60)

    # If the MV is not found, return the default value
    logging.warning(f"Warning: No refresh buffer found for MV: {mv_name}. Defaulting to 60 minutes.")
    return 60 