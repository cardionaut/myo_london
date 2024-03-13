import os
import hydra

import pandas as pd
import numpy as np
from loguru import logger
from omegaconf import DictConfig


@hydra.main(version_base=None, config_path='.', config_name='config_upload')
def main(config: DictConfig) -> None:
    template = pd.read_csv(config.template_file)
    template = template.loc[:, ~template.columns.str.contains('unnamed', case=False)]
    template_cols = template.columns

    strain = pd.read_excel(config.strain_file, skiprows=2) if config.strain_file is not None else None
    func = pd.read_excel(config.function_file) if config.function_file is not None else None
    dias = pd.read_excel(config.diastology_file) if config.diastology_file is not None else None

    if strain is not None:
        logger.info('Cleaning strain frame')
        strain = strain.iloc[:-1, :]  # TODO: remove this for future files (last row should be empty)
        strain = strain.rename(columns={'record_id': 'redcap_id'})
        strain = cleanup(strain)
        strain.columns = strain.columns.str.lower()

    if func is not None:
        logger.info('Cleaning func frame')
        func = func.rename(columns={'record_id': 'redcap_id'})
        func = cleanup(func)
        func.columns = func.columns.str.lower()

    if dias is not None:
        logger.info('Cleaning dias frame')
        dias = dias.rename(columns={'Redcap-ID': 'redcap_id'})
        dias = cleanup(dias)
        dias = dias.rename(
            columns={
                'RV_2d_edSR_long': 'RV_3d_edSR_long',
                'RV_2d_adSR_long': 'RV_3d_adSR_long',
                'RV_2d_e/a_dSR_long': 'RV_3d_e/a_dSR_long',
                'RV_2d_edVel_long': 'RV_3d_edVel_long',
                'RV_2d_adVel_long': 'RV_3d_adVel_long',
                'RV_2d_e/a_dVel_long': 'RV_3d_e/a_dVel_long',
            }
        )
        dias.columns = dias.columns.str.lower()
        dias.columns = dias.columns.str.replace('/', '')

    # Merge the dataframes
    merge_on = ['redcap_id', 'redcap_event_name', 'redcap_repeat_instance']
    if strain is not None:
        template = template.merge(strain, how='left', on=merge_on, suffixes=['_template', None])
    if func is not None:
        template = template.merge(func, how='left', on=merge_on, suffixes=['_template', None])
    if dias is not None:
        template = template.merge(dias, how='left', on=merge_on, suffixes=['_template', None])
    cols_to_drop = [col for col in template.columns if '_template' in col or col not in template_cols]
    template = template.drop(columns=cols_to_drop)
    template = template.sort_values(by=merge_on).reset_index(drop=True)

    # Save the data
    out_dir = os.path.dirname(config.template_file)
    file_basename = os.path.basename(config.template_file).split(".")[0]
    new_file_name = f'{file_basename}_merged.csv'

    if strain is not None:
        template['iqscore_func'] = template['iq_ft']
        template.loc[template['date_this_cmr'].notna(), 'cmr_examination_complete'] = 0
        template.loc[template['lv_edv'].notna(), 'cmr_cardiac_function_complete'] = 2
        template.loc[template['sax_available'].notna(), 'cmr_feature_tracking_complete'] = 2
        template.loc[template['ft_arrhythmia'] == 3, 'ft_arrhythmia'] = 0

    # Clean datatypes
    int_cols = [
        'redcap_repeat_instance',
        'sax_available',
        'lax_available',
        'ft_arrhythmia',
        'cmr_examination_complete',
        'cmr_cardiac_function_complete',
        'cmr_feature_tracking_complete',
    ]
    for col in int_cols:
        try:
            template[col] = template[col].astype('Int64')
        except KeyError:
            pass

    logger.info(f'Saving merged data to {os.path.join(out_dir, new_file_name)}')
    template.to_csv(os.path.join(out_dir, new_file_name), index=False)

    # for manual inspection
    template = template.dropna(axis=1, how='all').dropna(axis=0, thresh=4)
    template.to_csv(os.path.join(out_dir, f'{file_basename}_merged_inspection.csv'), index=False)


def cleanup(frame):
    frame.loc[:, :] = frame.loc[:, ~frame.columns.str.contains('unnamed', case=False)]

    frame.loc[frame['redcap_event_name'].isna(), 'redcap_event_name'] = 'baseline_arm_1'
    frame.loc[frame['redcap_event_name'] == 'fu', 'redcap_event_name'] = 'planned_followup_arm_1'
    frame.loc[frame['redcap_event_name'] == 'mace', 'redcap_event_name'] = 'mace_arm_1'
    frame.loc[frame['redcap_event_name'] == 'minor', 'redcap_event_name'] = 'minor_event_arm_1'

    frame['redcap_id'] = frame['redcap_id'].astype(str)
    frame['redcap_id'] = frame['redcap_id'].apply(lambda x: x.split(' ')[0] if ' ' in x else x)
    frame['redcap_id'] = frame['redcap_id'].apply(lambda x: x.split('(')[0] if '(' in x else x)
    frame['redcap_id'] = frame['redcap_id'].astype(int)

    frame = frame.replace('/', '', regex=True)
    frame = frame.replace('--', np.nan, regex=True)

    return frame


if __name__ == '__main__':
    main()
