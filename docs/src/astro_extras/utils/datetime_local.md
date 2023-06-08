# Datetime Local

[astro_extras Index](../../../README.md#astro_extras-index) /
`src` /
[Astro Extras](../index.md#astro-extras) /
[Utils](./index.md#utils) /
Datetime Local

> Auto-generated documentation for [src.astro_extras.utils.datetime_local](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py) module.

- [Datetime Local](#datetime-local)
  - [datetime_is_tz_aware](#datetime_is_tz_aware)
  - [datetime_to_local](#datetime_to_local)
  - [datetime_to_naive](#datetime_to_naive)
  - [datetime_to_tz](#datetime_to_tz)
  - [datetime_to_utc](#datetime_to_utc)
  - [localnow](#localnow)

## datetime_is_tz_aware

[Show source in datetime_local.py:13](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py#L13)

Check datetime is TZ-aware

#### Signature

```python
def datetime_is_tz_aware(dt: datetime) -> bool:
    ...
```



## datetime_to_local

[Show source in datetime_local.py:17](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py#L17)

Convert datetime of any type (TZ-aware or TZ-naive) to local TZ

#### Signature

```python
def datetime_to_local(dt: datetime) -> datetime:
    ...
```



## datetime_to_naive

[Show source in datetime_local.py:34](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py#L34)

Remove TZ component making datetime TZ-naive

#### Signature

```python
def datetime_to_naive(dt: datetime) -> datetime:
    ...
```



## datetime_to_tz

[Show source in datetime_local.py:25](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py#L25)

Convert datetime of any type (TZ-aware or TZ-naive) to given timezone

#### Signature

```python
def datetime_to_tz(dt: datetime, tz=None) -> datetime:
    ...
```



## datetime_to_utc

[Show source in datetime_local.py:21](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py#L21)

Convert datetime of any type (TZ-aware or TZ-naive) to UTC

#### Signature

```python
def datetime_to_utc(dt: datetime) -> datetime:
    ...
```



## localnow

[Show source in datetime_local.py:9](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/utils/datetime_local.py#L9)

Return datetime.now() in a local timezone (TZ-aware)

#### Signature

```python
def localnow() -> datetime:
    ...
```