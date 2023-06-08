# Utils

[astro_extras Index](../../../README.md#astro_extras-index) /
`src` /
[Astro Extras](../index.md#astro-extras) /
[Utils](./index.md#utils) /
Utils

> Auto-generated documentation for [src.astro_extras.utils.utils](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/utils.py) module.

- [Utils](#utils)
  - [ensure_table](#ensure_table)
  - [schedule_ops](#schedule_ops)
  - [split_table_name](#split_table_name)

## ensure_table

[Show source in utils.py:20](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/utils.py#L20)

Ensure an object passed in is a table

#### Signature

```python
def ensure_table(table: Union[str, Table], conn_id: Optional[str] = None) -> Table:
    ...
```



## schedule_ops

[Show source in utils.py:31](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/utils.py#L31)

Build a linked operators list

#### Signature

```python
def schedule_ops(ops_list: List[BaseOperator], num_parallel: int = 1) -> BaseOperator:
    ...
```



## split_table_name

[Show source in utils.py:13](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/utils.py#L13)

Splits table name to schema and table name

#### Signature

```python
def split_table_name(table: str) -> Tuple[Optional[str], str]:
    ...
```