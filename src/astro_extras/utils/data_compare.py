# Astro SDK Extras project
# (c) kol, 2023

""" Dataframe comparsion utility function """

import logging
import pandas as pd
from typing import Union, Iterable, Tuple

from airflow.settings import TIMEZONE
from airflow.exceptions import AirflowFailException

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

    logger = logging.getLogger('airflow.task')
    tz_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns]' and
                col not in exclude_cols]
    if tz_cols:
        logger.debug(f'Converting TZ-unaware timestamp columns {tz_cols} to timezone {TIMEZONE.name}')
        for col in tz_cols:
            df[col] = df[col].map(lambda t: t.tz_localize(TIMEZONE.name))

    return df

def _check_timestamp_types(df_a: pd.DataFrame, df_b: pd.DataFrame, exclude_cols: Iterable):
    """ Internal: check timestamp compatibility """

    tz_a = [col for col in df_a.columns if 'datetime64[ns' in str(df_a.dtypes[col]) and
                col not in exclude_cols]
    tz_b = [col for col in df_b.columns if 'datetime64[ns' in str(df_b.dtypes[col]) and
                col not in exclude_cols]
    for col in set(tz_a).intersection(tz_b):
        if df_a.dtypes[col] == 'datetime64[ns]' and df_b.dtypes[col] != 'datetime64[ns]':
            raise AirflowFailException(f'Incompatible timestamp column {col} type: ' \
                                   f'{df_a.dtypes[col]} on source, {df_b.dtypes[col]} on target')

def compare_datasets(
    df_src: pd.DataFrame, 
    df_trg: pd.DataFrame, 
    id_cols: Iterable | None = None, 
    exclude_cols: Iterable | None = None, 
    stop_on_first_diff: bool = True,
    logger: logging.Logger | None = None) -> Union[bool, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:

    """ Compare two dataframes by columns, their types and values.

    Args:
        df_src: Source dataframe

        df_trg: Target dataframe

        id_cols: List of columns treated as IDs.
            If empty, 1st column is considered to be ID

        exclude_cols: List of columns to be excluded from comparsion

        stop_on_first_diff: if `True`, any difference would stop the comparsion,
            otherwise it will continue and produce difference dataframes

        logger: logger object (if `None`, `airflow.task` logger will be used)

    Returns:
        If `stop_on_first_diff == True`, would return boolean flag whether
        the dataframes are the same (`True`) or not (`False`).

        Otherwise, will return 3 dataframes for new, changed and 
        deleted records. Any of them could be `None` meaning
        no corresponding difference detected.
    """

    logger = logger or logging.getLogger('airflow.task')

    orig_src_cols = list(df_src.columns)
    src_cols = df_src.columns.str.lower()
    trg_cols = df_trg.columns.str.lower()
    df_src.columns = src_cols
    df_trg.columns = trg_cols

    logger.debug(f'Source cols: {src_cols}')
    logger.debug(f'Target cols: {trg_cols}')

    if not exclude_cols:
        exclude_cols = []
    else:
        exclude_cols = [c.lower() for c in exclude_cols]
    exclude_cols.extend(['session_id', '_created', '_modified', '_deleted'])
    logger.debug(f'Excluded columns: {exclude_cols}')

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
    logger.debug(f'ID columns: {id_cols}')

    diff_cols = set(src_cols).symmetric_difference(trg_cols).difference(exclude_cols)
    if diff_cols:
        logger.warning(f'Сolumn difference detected: {diff_cols}.\n' + 
                       'Operation will continue, but may result in incomplete data transfer or errors.\n' +
                       'Review the datasets and correct structure to avoid that.')

    if df_src.shape[0] != df_trg.shape[0] and stop_on_first_diff:
        logger.info('Source and target datasets has different number of records, comparsion stopped')
        return False

    _check_timestamp_types(df_src, df_trg, exclude_cols)

    shared_cols = set(src_cols).intersection(trg_cols).difference(exclude_cols)
    logger.debug(f'Shared cols: {shared_cols}')

    df_src.index.name = '__src_idx__'
    df_trg.index.name = '__trg_idx__'
    df_deleted = None
    df_merged = pd.merge(df_src.reset_index(), df_trg.reset_index(), how='outer', on=id_cols,
                            copy=False, indicator=True, sort=False)
    if df_merged['_merge'].eq('right_only').any():
        logger.info('Target dataset contains records which do not exist on the source')
        if stop_on_first_diff:
            return False

        idx_list = df_merged[df_merged['_merge'] == 'right_only']['__trg_idx__']
        df_deleted = df_trg.iloc[idx_list]
        df_deleted.index.name = None

    def _iter_tuples():
        for row in df_merged.itertuples(index=False, name=None):
            d = {c: v for c, v in zip(df_merged.columns, row)}
            yield d.pop('__src_idx__'), d.pop('__trg_idx__'), d

    new_idx, upd_idx = [], []
    for src_idx, trg_idx, vals in _iter_tuples():
        src_vals, trg_vals = _split_row(vals, id_cols, shared_cols)
        key_val = [src_vals[c] for c in id_cols]

        logger.debug(f'Src {src_idx}: {src_vals}')
        logger.debug(f'Trg {trg_idx}: {trg_vals}')

        if not pd.isnull(vals.get('_deleted')) or vals.get('_merge') == 'right_only':
            logger.debug('Deleted record, skipping')
            continue
        if pd.isnull(trg_idx):
            if stop_on_first_diff:
                logger.info(f'New record at {key_val}')
                return False

            logger.debug(f'New record at {key_val}')
            new_idx.append(src_idx)
        else:
            diff_values = {k: {'source': src_vals[k], 'target': trg_vals[k]} for k in shared_cols \
                if not (pd.isnull(src_vals[k]) and pd.isnull(trg_vals[k])) \
                    and src_vals[k] != trg_vals[k]}
            if diff_values:
                key_val = [src_vals[c] for c in id_cols]
                if stop_on_first_diff:
                    logger.info(f'Diff at {id_cols}={key_val}: {diff_values}')
                    return False
                
                logger.debug(f'Diff at {id_cols}={key_val}: {diff_values}')
                upd_idx.append(src_idx)

    if stop_on_first_diff:
        logger.info(f'No differences detected')
        return True

    if not new_idx and not upd_idx and (df_deleted is None or df_deleted.empty):
        logger.info(f'No differences detected')
        return None, None, None

    if not new_idx:
        df_new = None
        logger.info(f'No new records on source')
    else:
        df_new = df_src.iloc[new_idx]
        df_new.index.name = None
        df_new.columns = orig_src_cols
        logger.info(f'{df_new.shape[0]} new records on source')

    if not upd_idx:
        df_upd = None
        logger.info(f'No modified records on source')
    else:
        df_upd = df_src.iloc[upd_idx]
        df_upd.index.name = None
        df_upd.columns = orig_src_cols
        logger.info(f'{df_upd.shape[0]} modified records on source')

    logger.info(f'{df_deleted.shape[0] if df_deleted is not None else 0} records deleted on target')

    return df_new, df_upd, df_deleted


def compare_timed_dict(df_src: pd.DataFrame, df_trg: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """ Compares two datasets as if they are timed dictionary data. Returns dataframes with records to open and to close """

    # Check target table is really a timed dictionary
    if not all([c in df_trg.columns for c in ['src_id', 'effective_from', 'effective_to']]):
        raise AirflowFailException(f'Destination table must contain `src_id`, `effective_from` and `effective_to` columns')

    # 1st column after `session_id` in a source table is considered to be ID
    src_id_col = [c for c in df_src.columns if c != 'session_id'][0]
    df_src.rename(columns={src_id_col: 'src_id'}, inplace=True)

    # Get the deltas
    df_new, df_upd, df_deleted = \
        compare_datasets(df_src, df_trg,
                            id_cols=['src_id'],
                            exclude_cols=['effective_from', 'effective_to'],
                            stop_on_first_diff=False)

    # New records are only for opening new record
    # Modified records are closed with opening new record
    # Deleted records are closed without opening new record
    effective_from = pd.Timestamp.now().floor('S')
    effective_to = effective_from - pd.Timedelta(seconds=1)

    df_closing = None
    if df_upd is not None and df_deleted is not None:
        df_closing = pd.concat([df_upd, df_deleted], ignore_index=True)
    elif df_upd is None and df_deleted is not None:
        df_closing = df_deleted
    elif df_upd is not None and df_deleted is None:
        df_closing = df_upd.copy()

    if df_closing is not None:
        df_closing['effective_to'] = effective_to

    df_opening = None
    if df_new is not None and df_upd is not None:
        df_opening = pd.concat([df_new, df_upd], ignore_index=True)
    elif df_new is None and df_upd is not None:
        df_opening = df_upd.copy()
    elif df_new is not None and df_upd is None:
        df_opening = df_new

    if df_opening is not None:
        df_opening['effective_from'] = effective_from
        df_opening['effective_to'] = None

    return df_opening, df_closing
