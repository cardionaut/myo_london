import pandas as pd
import numpy as np
from loguru import logger


class Cleaner:
    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data
        self.events = None

    def __call__(self) -> pd.DataFrame:
        self.drop_rows()
        self.rename_events()
        self.combine_rows()

        return self.data

    def drop_rows(self) -> None:
        """Remove any empty or undesired rows"""
        n_values_per_row = self.data.count(axis=1)
        rows_to_drop = n_values_per_row[
            n_values_per_row <= 2
        ].index  # remove rows with 2 or fewer entries (e.g. end_of_followup rows)
        self.data = self.data.drop(rows_to_drop)

    def rename_events(self) -> None:
        """Rename redcap events, i.e. shorten to use as col name suffix later"""
        rename_dict = {
            'baseline_arm_1': 'base',
            'planned_followup_arm_1': 'fu',
            'mace_arm_1': 'mace',
            'minor_event_arm_1': 'minor',
        }
        for old, new in rename_dict.items():  # faster than Series.replace for len(data) > 100
            self.data['redcap_event_name'] = self.data['redcap_event_name'].str.replace(old, new, regex=False)
        self.events = list(rename_dict.values())
        self.events.remove('base')  # base cols already exist
        self.data = self.data.drop('redcap_repeat_instrument', axis=1)  # empty anyway

    def combine_rows(self) -> None:
        """Combine rows belonging to the same patient in one row"""
        base_data = self.data[self.data['redcap_event_name'].str.contains('base')]
        for event in self.events:
            event_data = self.data[self.data['redcap_event_name'].str.contains(event)]
            # find relevant (non-empty) columns for each event
            relevant_cols = event_data.dropna(axis=1, how='all').columns.drop(
                ['redcap_event_name', 'redcap_repeat_instance']
            )
            # find max number of each event type
            max_events = event_data['redcap_id'].value_counts().max()
            for event_number in range(1, max_events + 1):
                tmp = pd.DataFrame(event_data[event_data['redcap_repeat_instance'] == event_number][relevant_cols])
                base_data = base_data.merge(
                    tmp, how='outer', on='redcap_id', suffixes=[None, f'_{event}_{event_number}']
                )

        self.data = base_data.dropna(axis=1, how='all').drop('redcap_event_name', axis=1)

