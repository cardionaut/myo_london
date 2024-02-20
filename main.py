import os
import hydra

import pandas as pd
from loguru import logger
from omegaconf import DictConfig
from pathlib import Path

from utils.combine_rows import CombineRows
from utils.cleaner import Cleaner


@hydra.main(version_base=None, config_path='.', config_name='config')
def main(config: DictConfig) -> None:
    file_path = config.combine_rows.file_path
    path, extension = os.path.splitext(file_path)
    new_file_name = f'{path}_rowsCombined.xlsx'
    out_dir = os.path.dirname(file_path)

    if config.combine_rows.active:
        logger.info(f'Combining rows in {file_path}')
        if extension == '.xlsx':
            data = pd.read_excel(file_path)
        elif extension == '.csv':
            data = pd.read_csv(file_path)
        else:
            logger.error(f'File type {extension} not supported')
            return
        combine_rows = CombineRows(data)
        new_data = combine_rows()
        with pd.ExcelWriter(os.path.join(out_dir, new_file_name), datetime_format='DD/MM/YYYY') as writer:
            new_data.to_excel(writer, index=False)
    else:
        try:
            logger.info(f'Loading combined data from {new_file_name}')
            new_data = pd.read_excel(os.path.join(out_dir, new_file_name))
        except FileNotFoundError:
            logger.error(f'File {new_file_name} not found. Please run the combine_rows step first.')
            return

    if config.cleaner.active:
        logger.info(f'Cleaning and combining {file_path}')
        cleaner = Cleaner(config, new_data)
        cleaned_data = cleaner()
        logger.info(f'Saving cleaned data of shape {cleaned_data.shape}')
        with pd.ExcelWriter(os.path.join(out_dir, f'{path}_cleaned.xlsx'), datetime_format='DD/MM/YYYY') as writer:
            cleaned_data.to_excel(writer, index=False)


if __name__ == '__main__':
    main()
