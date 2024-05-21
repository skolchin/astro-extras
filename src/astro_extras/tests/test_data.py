import random
import string
import hashlib
import logging
import pandas as pd
from typing import List
from astro.sql.table import Table, Metadata
from sqlalchemy import Table as sqla_table
from sqlalchemy import Boolean, Column, Float, MetaData, String, Integer, DateTime

from astro.databases.base import BaseDatabase
from astro.databases.postgres import PostgresDatabase

class TestData:
    """Class for generating test data and creating tables in a database using astro and sqlalchemy."""

    def __init__(
        self, 
        conn_id: str, 
        dest_conn_id: str = None, 
        num_cols: int = 10, 
        num_rows: int = 20, 
        num_tables: int = None, 
        name_tables: List[str]=None, 
        schema: str = 'public',
        with_primary_key: bool = False,
        with_session_id: bool = False,
        with_ods_fields: bool = False) -> None:
        """
        Initialize the TestData object with connection information and parameters.

        Args:
            conn_id (str): The connection ID to the database.
            dest_conn_id (str, optional): The destination connection ID for the target database.
            num_cols (int, optional): The number of columns for each generated table (default is 10).
            num_rows (int, optional): The number of rows for each generated table (default is 20).
            num_tables (int, optional): The number of tables to generate (default is None).
            name_tables (List[str], optional): List of table names to use (default is None).
            schema (str, optional): The schema for the database tables (default is 'public').
        """

        self.conn_id: str = conn_id
        self.dest_conn_id = dest_conn_id
        self.num_cols = num_cols
        self.num_rows = num_rows
        self.num_tables = num_tables
        self.name_tables = name_tables
        self.metadata: MetaData = Metadata(schema=schema)
        self.with_primary_key = with_primary_key
        self.with_session_id = with_session_id
        self.with_ods_fields = with_ods_fields

        if not self.num_tables and not self.name_tables:
            raise ValueError('Either `num_tables` or `name_tables` parameters must be provided')

        self.log = logging.getLogger('airflow.task')
        self.tables = self.generate_tables()

    def generate_random_data(self, num_rows: int, columns: List[Column]) -> pd.DataFrame:
        """
        Generate random data for each column in a DataFrame.

        Args:
            num_rows (int): The number of rows to generate.
            columns (List[Column]): List of sqlalchemy Column objects representing table columns.

        Returns:
            pd.DataFrame: A DataFrame containing randomly generated data.
        """
        data = {}
        for column in columns:
            col_name = column.name
            match column.type:
                case String():
                    data[col_name] = [random.choice(string.ascii_letters) for _ in range(num_rows)]
                case Integer() if column.autoincrement:
                    pass
                case Integer() if col_name == 'session_id':
                    pass
                case Integer():
                    data[col_name] = [random.randint(1, 100) for _ in range(num_rows)]
                case Float():
                    data[col_name] = [round(random.uniform(1, 100), 2) for _ in range(num_rows)]
                case Boolean():
                    data[col_name] = [random.choice([True, False]) for _ in range(num_rows)]
                case DateTime() if col_name in ('_created', '_deleted', '_modified'):
                    pass
                case DateTime():
                    data[col_name] = [pd.Timestamp.now() - pd.Timedelta(days=random.randint(30)) for _ in range(num_rows)]
                case _:
                    raise ValueError(f"Unsupported data type {column.data_type}")
        return pd.DataFrame(data)

    def generate_columns(self, num_cols: int, with_primary_key: bool = False, with_ods_fields: bool = False) -> List[Column]:
        """
        Generate a list of columns with names generated using MD5 hash function.

        Args:
            num_cols (int): The number of columns to generate.

        Returns:
            List[Column]: A list of sqlalchemy Column objects.
        """
        columns = []
        if with_primary_key:
            columns.append(Column('id', Integer, primary_key=True, autoincrement=True))
        if with_ods_fields:
            columns.append(Column('_deleted', DateTime))
            columns.append(Column('_modified', DateTime))
        columns.extend([Column(f'col_{i+1:03d}', String) for i in range(num_cols)])
        return columns

    def create_astro_table_object(self, name: str, metadata: Metadata, columns: List[Column]) -> Table:
        """
        Create a new astro table object with the given name, metadata, and columns.

        Args:
            name (str): The name of the table.
            metadata (Metadata): The Metadata object representing the table's schema.
            columns (List[Column]): List of sqlalchemy Column objects representing table columns.

        Returns:
            Table: An astro Table object.
        """
        return Table(name=name, metadata=metadata, columns=columns)

    def get_astro_database(self, conn_id: str) -> BaseDatabase:
        """
        Make an Astro-SDK database object using provided connection ID.

        Args:
            conn_id (str): The connection ID to the database.

        Returns:
            BaseDatabase: An instance of a database connection class (e.g., PostgresDatabase).
        """
        return PostgresDatabase(conn_id=conn_id)

    def generate_tables(self) -> List[Table]:
        """
        Generate a list of tables with specified columns and names.

        Args:
            num_cols (int): The number of columns for each generated table.
            num_rows (int): The number of rows for each generated table.
            num_tables (int, optional): The number of tables to generate (default is None).
            name_tables (List[str], optional): List of table names to use (default is None).

        Returns:
            List[Table]: A list of astro Table objects.
        """
        tables = []

        def create_table(table_name: str, num_cols: int, metadata):
            # Create a new astro table object with the given name, metadata, and columns
            columns = self.generate_columns(num_cols, with_primary_key=self.with_primary_key, with_ods_fields=self.with_ods_fields)
            table = self.create_astro_table_object(table_name, metadata, columns)
            return table

        if self.name_tables:
            # If a list of names is provided, create tables with these names
            for name in self.name_tables:
                table = create_table(name, self.num_cols, self.metadata)
                tables.append(table)
        else:
            # Otherwise, create tables with default names
            if self.num_tables is None:
                raise ValueError("You must specify the number of tables to generate.")
            
            # Otherwise, create tables with default names
            for i in range(self.num_tables):
                table_name = f'test_table_{i+1}'
                table = create_table(table_name, self.num_cols, self.metadata)
                tables.append(table)
        
        return tables

    def append_tables_to_db(self, create_target_table_without_data: bool = True):
        """
        Save generated tables to the source and target database.

        Args:
            create_target_table_without_data (bool): Whether to create the target table without data (default is True).
        """

        # Initialize database connections
        source_db = self.get_astro_database(self.conn_id)
        target_db = self.get_astro_database(self.dest_conn_id)
        
        # Open connections
        with source_db.sqlalchemy_engine.connect() as src_conn, target_db.sqlalchemy_engine.connect() as tgt_conn:
            for table in self.tables:
                # Make table on source
                t = sqla_table(table.name, MetaData(schema=self.metadata.schema), *table.columns)
                src_conn.execute(f'drop table if exists {source_db.get_table_qualified_name(table)}')
                t.create(src_conn)
                self.log.info(f'Table {table.name} created in {self.conn_id} database with columns: {[c.name for c in table.columns]}')

                # save generated data to source table
                data = self.generate_random_data(self.num_rows, table.columns)
                data.to_sql(table.name, con=src_conn, schema=self.metadata.schema, if_exists='append', index=False)
                self.log.info(f'{len(data)} records saved to table {table.name} ')

                # drop target table and _a view
                tgt_conn.execute(f'drop view if exists {target_db.get_table_qualified_name(table)}_a cascade')
                tgt_conn.execute(f'drop table if exists {target_db.get_table_qualified_name(table)} cascade')

                # Create target table
                if create_target_table_without_data:
                    # Create target table without data
                    target_columns = [Column(c.name, c.type, primary_key=c.primary_key, autoincrement=c.autoincrement) for c in table.columns]
                    if self.with_session_id:
                        # add session_id as 1st column
                        if target_columns[0].primary_key:
                            target_columns[0].primary_key = False
                            target_columns[0].autoincrement = False
                        target_columns.insert(0, Column('session_id', Integer, primary_key=False))

                    sqla_table(table.name, MetaData(schema=self.metadata.schema), *target_columns).create(tgt_conn)
                    self.log.info(f'Table {table.name} created in {self.dest_conn_id} database with columns: {[c.name for c in target_columns]}')
                else:
                    # Create target table with data
                    sqla_table(table.name, MetaData(schema=self.metadata.schema), *table.columns).create(tgt_conn)
                    self.log.info(f'Table {table.name} created in {self.conn_id} database with columns: {[c.name for c in table.columns]}')

                    data.to_sql(table.name, con=tgt_conn, schema=self.metadata.schema, if_exists='append', index=False)
                    self.log.info(f'{len(data)} records saved to table {table.name} ')

                # create actual data view (_a)
                full_name = target_db.get_table_qualified_name(table)
                if not self.with_session_id:
                    tgt_conn.execute(f'create or replace view {full_name}_a as ' \
                                    f'select * from {full_name}')
                elif not self.with_ods_fields:
                    tgt_conn.execute(f'create or replace view {full_name}_a as ' \
                                    f'select * from {full_name} ' \
                                    f'where session_id = (select max(session_id) from public.sessions where status=\'success\')')
                else:
                    tgt_conn.execute(f'create or replace view {full_name}_a as ' \
                                    f'with d as (' \
                                        f'select distinct on (r.id) r.id, r.session_id, r._deleted from public.sessions s ' \
                                        f'inner join {full_name} r on r.session_id=s.session_id and s.status=\'success\'' \
                                        f'order by r.id, r.session_id desc) ' \
                                    f'select t.* from {full_name} t inner join d on t.id = d.id and t.session_id = d.session_id and d._deleted is null')

    def renew_tables_in_source_db(self):
        """ Overwrite data in the source database with random values """
        
        assert self.name_tables, 'Table names must be provided'

        # Initialize database connections
        source_db = self.get_astro_database(self.conn_id)
        
        # Open connections
        with source_db.sqlalchemy_engine.connect() as src_conn:
            # Load metadata
            meta = MetaData(src_conn)
            meta.reflect()

            # Loop over tables
            for table_name in self.name_tables:
                # retrieve table
                table = meta.tables[table_name]

                # Generate data for table
                data = self.generate_random_data(self.num_rows, table.columns)

                # Upload new data
                src_conn.execute(f'truncate table {source_db.get_table_qualified_name(table)}')
                data.to_sql(name=table.name, con=src_conn, schema=meta.schema, if_exists='append', index=False)

    def create_tables(self, tables: List[Table], db: PostgresDatabase | None = None) -> List[Table]:
        """
        Create tables in the database.

        Args:
            tables (List[Table]): A list of astro Table objects to create.
            conn (PostgresDatabase, optional): A specific database connection (default is None).

        Returns:
            List[Table]: A list of created astro Table objects.
        """
        if db is None:
            # Connect to PostgreSQL database using connection ID
            db = self.get_astro_database(self.conn_id)

        # Create each table in the database
        for table in tables:
            db.create_table(table)

        return tables

    def drop_tables(self, tables: List[str | Table | sqla_table]) -> List[str]:
        """
        Drop specified tables from the database.

        Args:
            tables: A list of table names or astro Table objects to drop.

        Returns:
            List[str]: A list of dropped table names.
        """
        # Connect to PostgreSQL database using connection ID
        db = self.get_astro_database(self.conn_id)

        dropped_tables = []

        for item in tables:
            match item:
                case Table():
                    db.drop_table(item)
                    dropped_tables.append(item.name)
                case str():
                    db.execute(f"DROP TABLE IF EXISTS {item}")
                    dropped_tables.append(item)
                case sqla_table():
                    item.drop(db.sqlalchemy_engine)
                case _:
                    raise ValueError("All items in the list must be Table objects or strings representing table names.")

        return dropped_tables

    def get_table_data(
        self,
        table_name: str,
        select_fields: str = "*",
        limit: int = None,
        order_by: str = None,
        ascending: bool = True,
        group_by: str = None
    ) -> pd.DataFrame:
        """
        Retrieve data from a specific table with optional parameters and return it as a Pandas DataFrame.

        Args:
            table_name (str): The name of the table to retrieve data from.
            select_fields (str, optional): Fields to select in the SQL query (default is '*').
            limit (int, optional): Number of records to limit the query to. Default is None (no limit).
            order_by (str, optional): Field to use for sorting the result.
            ascending (bool, optional): Sorting direction, True for ascending, False for descending.
            group_by (str, optional): Field to use for grouping the result.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the retrieved data.
        """
        # Connect to the database using connection ID
        db = self.get_astro_database(self.conn_id)
        
        # Construct the SQL query
        query = f"SELECT {select_fields} FROM {table_name}"
        
        if group_by:
            query += f" GROUP BY {group_by}"
        
        if order_by:
            query += f" ORDER BY {order_by} {'ASC' if ascending else 'DESC'}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute the SQL query and retrieve the data
        data = db.run_sql(query)
        return pd.DataFrame(data)

    def get_table_names(self, schema: str = 'public') -> List[str]:
        """
        Retrieve the names of all tables in the specified schema of the database.

        Args:
            schema (str, optional): The schema name to retrieve table names from. Default is 'public'.

        Returns:
            List[str]: A list of table names.
        """
        db = self.get_astro_database(self.conn_id)
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
        result = db.run_sql(query)
        table_names = [row[0] for row in result]
        return table_names

    def truncate_tables(self, table_names: List[str]) -> None:
        """
        Truncate (remove all rows) from the specified tables.

        Args:
            table_names (List[str]): A list of table names to truncate.
        """
        db = self.get_astro_database(self.conn_id)
        for table_name in table_names:
            query = f"TRUNCATE TABLE {table_name}"
            db.run_sql(query)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

