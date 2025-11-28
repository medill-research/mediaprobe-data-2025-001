
import logging
import yaml
from pathlib import Path

# Set up logging configurations
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Get the location of all data
DATA_PATH = Path(__file__).joinpath("..", "..", "data").resolve()

# Get the location of all scripts
SCRIPTS_PATH = Path(__file__).joinpath("..", "..", "scripts").resolve()

# Get the location of all configuration files
CONFIGS_PATH = Path(__file__).joinpath("..", "..", "configs").resolve()

# Get preprocessing configurations
PREPROCESSING_CONFIGS_PATH = CONFIGS_PATH.joinpath("preprocessing_configs.yaml").resolve()
PREPROCESSING_CONFIGS = yaml.safe_load(open(PREPROCESSING_CONFIGS_PATH, "r").read())
