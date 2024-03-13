import re

import pandas as pd
import numpy as np
from loguru import logger


class Cleaner:
    def __init__(self, config, data: pd.DataFrame) -> None:
        self.cols_to_drop = config.cleaner.cols_to_drop
        mace_types = config.cleaner.mace_types
        self.data = data
        self.mace_types = {
            mace_type: [col for col in self.data.columns if col.startswith(f'{mace_type}_mace')]
            for mace_type in mace_types
        }
        self.fu_time_cols = [col for col in self.data.columns if col.startswith('time_inc_fu')]

    def __call__(self) -> pd.DataFrame:
        self.time_to_censor()
        self.drop_cols()

        return self.data

    def drop_cols(self) -> None:
        """Remove any empty or undesired columns"""
        # turns strings into NaN -> easier to drop cols
        patient_year = self.data['patient_year']  # don't want to drop this
        cmr = self.data['date_cmr']
        this_cmr = self.data['date_this_cmr']
        # recommendations = self.data['whichrecommendations_2']
        self.data = self.data.apply(pd.to_numeric, errors='coerce')
        self.data['patient_year'] = patient_year
        self.data['date_cmr'] = cmr
        self.data['date_this_cmr'] = this_cmr
        # self.data['whichrecommendations_2'] = recommendations
        # cols to drop according to config
        self.data = self.data.drop(self.cols_to_drop, axis=1)
        # empty cols
        self.data = self.data.dropna(axis=1, how='all')
        # date cols
        date_cols = self.data.columns[self.data.columns.str.contains('date', case=False)]
        date_cols = [col for col in date_cols if col not in ['date_cmr', 'date_this_cmr']]
        self.data = self.data.drop(date_cols, axis=1)
        # follow up cols
        fu_cols = [col for col in self.data.columns if '_fu_' in col and not col.endswith('_fu_1')]
        self.data = self.data.drop(fu_cols, axis=1)
        # mace cols
        mace_regex = re.compile('.*_mace_\d\d?$')
        mace_cols = list(filter(mace_regex.search, self.data.columns))
        self.data = self.data.drop(mace_cols, axis=1)
        # minor cols
        minor_regex = re.compile('.*_minor_\d\d?$')
        minor_cols = list(filter(minor_regex.search, self.data.columns))
        self.data = self.data.drop(minor_cols, axis=1)

    def time_to_censor(self) -> None:
        """Create time_to_mace columns"""
        fu_time_cols = self.fu_time_cols[::-1]  # reverse order to get time to last follow up
        self.data['ob_time_days'] = self.data['ob_time_endfu'] * 365

        for i, row in self.data.iterrows():
            mace_type_years = {mace_type: [] for mace_type in self.mace_types.keys()}
            fu_days = None
            global_censor_mace = []
            global_censor_fu = []
            global_mace = False
            for mace_type, mace_cols in self.mace_types.items():
                for mace_col in mace_cols:
                    mace_number = mace_col.split('_')[-1]
                    if row[mace_col] == 1:
                        mace_type_years[mace_type].append(row[f'time_to_mace_days_mace_{mace_number}'])
                        global_mace = True

            if not global_mace:  # if no mace, use last follow up
                for fu_col in fu_time_cols:
                    if not pd.isna(row[fu_col]):
                        fu_days = row[fu_col]
                        break

            for mace_type, mace_days in mace_type_years.items():
                if mace_days:
                    self.data.loc[i, f'time_to_{mace_type}'] = min(mace_days)
                    self.data.loc[i, f'{mace_type}'] = 1
                    global_censor_mace.append(min(mace_days))
                elif fu_days:
                    self.data.loc[i, f'time_to_{mace_type}'] = max(fu_days, row['ob_time_days'])
                    self.data.loc[i, f'{mace_type}'] = 0
                    global_censor_fu.append(max(fu_days, row['ob_time_days']))
                else:
                    self.data.loc[i, f'time_to_{mace_type}'] = row['ob_time_days']
                    self.data.loc[i, f'{mace_type}'] = 0
                    global_censor_fu.append(row['ob_time_days'])

            if global_censor_mace:
                global_censor = min(global_censor_mace)
            elif global_censor_fu:
                global_censor = max(global_censor_fu)
            else:
                global_censor = None

            self.data.loc[i, 'time_to_censor'] = global_censor
            self.data.loc[i, 'mace'] = int(global_mace)
