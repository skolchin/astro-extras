from __future__ import annotations

import pandas as pd
import sqlalchemy
from oracledb import OperationalError, init_oracle_client
from airflow.configuration import conf
from airflow.providers.oracle.hooks.oracle import OracleHook
from astro.settings import SECTION_KEY
from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.options import LoadOptions
from astro.table import BaseTable, Metadata
from astro.utils.compat.functools import cached_property

DEFAULT_CONN_ID = OracleHook.default_conn_name


class OracleDatabase(BaseDatabase):
    def __init__(
        self,
        conn_id: str = DEFAULT_CONN_ID,
        table: BaseTable | None = None,
        load_options: LoadOptions | None = None, 
        thick_client: bool = conf.getboolean(SECTION_KEY, "thick_oracle_client", fallback=False),

    ):
        super().__init__(conn_id)
        self.table = table
        self.load_options = load_options
        self.thick_client = thick_client
        if self.thick_client:
            init_oracle_client()

    @cached_property
    def hook(self) -> OracleHook:
        """Retrieve Airflow hook to interface with the oracle database."""
        return OracleHook(oracle_conn_id=self.conn_id)

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
        """
        Given a table, check if the table has metadata.
        If the metadata is missing, and the database has metadata, assign it to the table.
        If the table schema was not defined by the end, retrieve the user-defined schema.
        This method performs the changes in-place and also returns the table.

        :param table: Table to potentially have their metadata changed
        :return table: Return the modified table
        """
        if table.metadata and table.metadata.is_empty() and self.default_metadata:
            table.metadata = self.default_metadata
        return table

    @property
    def default_metadata(self) -> Metadata:
        conn = self.hook.get_conn()
        return Metadata(database=conn.db_name, schema=conn.username)

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: BaseTable,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        self._assert_not_empty_df(source_dataframe)

        schema = None
        if target_table.metadata:
            schema = target_table.metadata.schema

        source_dataframe.to_sql(
            target_table.name,
            con=self.sqlalchemy_engine,
            schema=schema,
            if_exists=if_exists,
            chunksize=chunk_size,
            method=None,
            index=False,
        )

    @property
    def sql_type(self) -> str:
        return "oracle"

    def table_exists(self, table: BaseTable) -> bool:
        """
        Check if a table exists in the database

        :param table: Details of the table we want to check that exists
        """
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)

        schema = None
        if table and table.metadata:
            schema = table.metadata.schema
        return bool(inspector.dialect.has_table(self.connection, table.name, schema=schema))

    def schema_exists(self, schema) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: A schema, which is equivalent for username in Oracle
        """
        try:
            schema_result = self.hook.run(
                "SELECT username FROM dba_users WHERE "
                "UPPER(username) = UPPER(%(schema_name)s);",
                parameters={"schema_name": schema},
                handler=lambda x: [y[0] for y in x.fetchall()],
            )
            return len(schema_result) > 0
        except OperationalError:
            return False

    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        if table.metadata and table.metadata.schema:
            qualified_name = f"{table.metadata.schema.upper()}.{table.name.upper()}"
        else:
            qualified_name = table.name.upper()
        return qualified_name

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ) -> None:
        """
        Merge the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be merged to the target_table
        :param target_table: Contains the destination table in which the rows will be merged
        :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
        :param target_conflict_columns: List of cols where we expect to have a conflict while combining
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        statement = (
            "INSERT INTO {target_table}"
            "({target_columns})  "
            "SELECT {source_columns} "
            "FROM {source_table}  "
            "ON DUPLICATE KEY UPDATE {merge_vals};"
        )

        source_column_names = list(source_to_target_columns_map.keys())
        target_column_names = list(source_to_target_columns_map.values())

        target_identifier_enclosure = ""
        source_identifier_enclosure = ""

        join_conditions = ",".join(
            [
                f"{target_table.name}.{target_identifier_enclosure}{t}{target_identifier_enclosure}="
                f"{source_table.name}.{source_identifier_enclosure}{s}{source_identifier_enclosure}"
                for s, t in source_to_target_columns_map.items()
            ]
        )
        statement = statement.replace("{merge_vals}", join_conditions)

        statement = statement.format(
            target_columns=",".join(target_column_names),
            target_table=target_table.name,
            source_columns=",".join(source_column_names),
            source_table=source_table.name,
        )

        self.run_sql(sql=statement)

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: database.schema.table.name
        """
        schema = self.hook.get_connection(self.conn_id).schema
        if table.metadata and table.metadata.schema:
            schema = table.metadata.schema
        return f"{schema}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: mssql://localhost:3306
        """
        conn = self.hook.get_connection(self.conn_id)
        return f"{self.sql_type}://{conn.host}:{conn.port}"

    def openlineage_dataset_uri(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace()}/{self.openlineage_dataset_name(table=table)}"
