import os
import hydra

import pandas as pd
from loguru import logger
from omegaconf import DictConfig, OmegaConf
from pathlib import Path

from utils.cleaner import Cleaner

@hydra.main(version_base=None, config_path='.', config_name='config')
def main(config: DictConfig) -> None:
    file_path = config.file_path
    data = pd.read_excel(file_path)

    cleaner = Cleaner(data)
    new_data = cleaner()
    logger.debug(f'\n{new_data.iloc[:10, :]}')

    out_dir = os.path.dirname(file_path)
    new_file_name = f'{Path(file_path).stem}_cleaned.xlsx'
    new_data.to_excel(os.path.join(out_dir, new_file_name), index=False)

if __name__ == '__main__':
    main()