import os
import hydra

import pandas as pd
from loguru import logger
from omegaconf import DictConfig, OmegaConf

@hydra.main(version_base=None, config_path='.', config_name='config')
def main(config: DictConfig) -> None:
    file_path = config.file_path
    data = pd.read_excel(file_path)
    


if __name__ == '__main__':
    main()