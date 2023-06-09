# Astro SDK Extras project
# (c) kol, 2023

""" Dataframe comparsion utility function """

import logging
import pandas as pd
from typing import Union, Iterable, Tuple

from airflow.settings import TIMEZONE
from airflow.exceptions import AirflowFailException

_logger = logging.getLogger('airflow.task')

def _split_row(vals, id_cols, shared_cols):
    """ Internal - split row by subsets """
    split_vals = {'x': {}, 'y': {}, '': {}}

    for id_key in id_cols:
        if id_key in vals:
            split_vals['x'][id_key] = vals[id_key]
            split_vals['y'][id_key] = vals[id_key]

    for shared_key in shared_cols:
        if shared_key in vals:
            split_vals[''][shared_key] = vals[shared_key]
        for suffix in ('x', 'y'):
            key = f'{shared_key}_{suffix}'
            if key in vals:
                split_vals[suffix][shared_key] = vals[key]
    return split_vals['x'], split_vals['y']

def _adjust_timestamps(df: pd.DataFrame, exclude_cols: Iterable):
    """ Internal: adjust timestamps to common TZ """

    tz_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns]' and
                col not in exclude_cols]
    if tz_cols:
        _logger.debug(f'Converting TZ-unaware timestamp columns {tz_cols} to timezone {TIMEZONE.name}')
        for col in tz_cols:
            df[col] = df[col].map(lambda t: t.tz_localize(TIMEZONE.name))

    return df

def _check_timestamp_types(df_a: pd.DataFrame, df_b: pd.DataFrame, exclude_cols: Iterable):
    """ Internal: check timestamp compatibility """

    tz_a = [col for col in df_a.columns if 'datetime64[ns' in str(df_a[col].dtype) and
                col not in exclude_cols]
    tz_b = [col for col in df_b.columns if 'datetime64[ns' in str(df_b[col].dtype) and
                col not in exclude_cols]
    for col in set(tz_a).intersection(tz_b):
        if df_a[col].dtype == 'datetime64[ns]' and df_b[col].dtype != 'datetime64[ns]':
            raise AirflowFailException(f'Incompatible timestamp column {col} type: ' \
                                   f'{df_a[col].dtype} on source, {df_b[col].dtype} on target')

def compare_datasets(
    df_src: pd.DataFrame, 
    df_trg: pd.DataFrame, 
    id_cols: Iterable = None, 
    exclude_cols: Iterable = None, 
    stop_on_first_diff: bool = True) -> Union[bool, Tuple[pd.DataFrame, pd.DataFrame]]:

    """ Compare datasets
    """

    orig_src_cols = list(df_src.columns)
    src_cols = df_src.columns.str.lower()
    trg_cols = df_trg.columns.str.lower()
    df_src.columns = src_cols
    df_trg.columns = trg_cols
    _logger.debug(f'Source cols: {src_cols}')
    _logger.debug(f'Target cols: {trg_cols}')

    if not exclude_cols:
        exclude_cols = []
    else:
        exclude_cols = [c.lower() for c in exclude_cols]
    exclude_cols.extend(['session_id', '_created', '_modified', '_deleted'])
    _logger.debug(f'Excluded columns: {exclude_cols}')

    if not id_cols:
        id_cols = [c for c in src_cols if c not in exclude_cols][:1]
    else:
        id_cols = [c.lower() for c in id_cols]
        diff_cols = set(id_cols).difference(src_cols)
        if diff_cols:
            raise AirflowFailException(f'ID columns missing from source dataset: {diff_cols}')
        diff_cols = set(id_cols).difference(trg_cols)
        if diff_cols:
            raise AirflowFailException(f'ID columns missing from target dataset: {diff_cols}')
    _logger.debug(f'ID columns: {id_cols}')

    diff_cols = set(src_cols).symmetric_difference(trg_cols).difference(exclude_cols)
    if diff_cols:
        _logger.warning(f'Diff in columns detected: {diff_cols}.\n' + 
                       'Operation will continue, but may result in incomplete data transfer or errors.\n' +
                       'Review the datasets and correct structure to avoid that.')

    if df_src.shape[0] != df_trg.shape[0] and stop_on_first_diff:
        _logger.info('Number of records in source and target dataset is different')
        return False

    _check_timestamp_types(df_src, df_trg, exclude_cols)

    shared_cols = set(src_cols).intersection(trg_cols).difference(exclude_cols)
    _logger.debug(f'Shared cols: {shared_cols}')

    df_src.index.name = '__src_idx__'
    df_trg.index.name = '__trg_idx__'
    df_deleted = None
    df_merged = pd.merge(df_src.reset_index(), df_trg.reset_index(), how='outer', on=id_cols,
                            copy=False, indicator=True, sort=False)
    if df_merged['_merge'].eq('right_only').any():
        _logger.info('Target dataset contains records not present on the source')
        if stop_on_first_diff:
            return False

        idx_list = df_merged[df_merged['_merge'] == 'right_only']['__trg_idx__']
        df_deleted = df_trg.iloc[idx_list]
        df_deleted.index.name = None

    def _iter_tuples():
        for row in df_merged.itertuples(index=False, name=None):
            d = {c: v for c, v in zip(df_merged.columns, row)}
            yield d.pop('__src_idx__'), d.pop('__trg_idx__'), d

    idx_list = []
    for src_idx, trg_idx, vals in _iter_tuples():
        src_vals, trg_vals = _split_row(vals, id_cols, shared_cols)
        key_val = [src_vals[c] for c in id_cols]

        _logger.debug(f'Src {src_idx}: {src_vals}')
        _logger.debug(f'Trg {trg_idx}: {trg_vals}')

        if not pd.isnull(vals.get('_deleted')) or vals.get('_merge') == 'right_only':
            _logger.debug('Deleted record, skipping')
            continue
        if pd.isnull(trg_idx):
            if stop_on_first_diff:
                _logger.info(f'New record at {key_val}')
                return False

            _logger.debug(f'New record at {key_val}')
            idx_list.append(src_idx)
        else:
            diff_values = {k: {'source': src_vals[k], 'target': trg_vals[k]} for k in shared_cols \
                if not (pd.isnull(src_vals[k]) and pd.isnull(trg_vals[k])) \
                    and src_vals[k] != trg_vals[k]}
            if diff_values:
                key_val = [src_vals[c] for c in id_cols]
                if stop_on_first_diff:
                    _logger.info(f'Diff at {id_cols}={key_val}: {diff_values}')
                    return False
                
                _logger.debug(f'Diff at {id_cols}={key_val}: {diff_values}')
                idx_list.append(src_idx)

    if stop_on_first_diff:
        _logger.info(f'No differences detected')
        return True

    if not idx_list and (df_deleted is None or df_deleted.empty):
        _logger.info(f'No differences detected')
        return None, None

    if not idx_list:
        df_merged = None
    else:
        df_merged = df_src.iloc[idx_list]
        df_merged.index.name = None
        df_merged.columns = orig_src_cols

    _logger.info(f'{df_merged.shape[0] if df_merged is not None else 0} new or modified records on source')
    _logger.info(f'{df_deleted.shape[0] if df_deleted is not None else 0} records deleted on target')

    return df_merged, df_deleted
