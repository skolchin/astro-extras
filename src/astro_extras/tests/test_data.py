import random
import string
import hashlib
import pandas as pd

from typing import List, Union
from astro.sql.table import Table, Metadata
from sqlalchemy import Table as sqla_table
from sqlalchemy import Boolean, Column, Float, MetaData, String, Integer

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
        db: str = 'postgres') -> None:
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
            db (str, optional): The type of database (e.g., 'postgres', 'duckdb', 'mysql', 'mssql', 'sqlite', 'snowflake').
        """

        self.conn_id: str = conn_id
        self.dest_conn_id = dest_conn_id
        self.num_cols = num_cols
        self.num_rows = num_rows
        self.num_tables = num_tables
        self.name_tables = name_tables
        self.metadata: MetaData = Metadata(schema=schema)
        self.db = db
        self.validate_params()

    def validate_params(self):
        if not self.num_tables and not self.name_tables:
            raise ValueError('One of the parameters must be filled: num_tables or name_tables')

    @staticmethod
    def generate_random_data(num_rows: int, columns: List[Column]) -> pd.DataFrame:
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
            if isinstance(column.type, String):
                data[col_name] = [random.choice(string.ascii_letters) for _ in range(num_rows)]
            elif isinstance(column.type, Integer):
                data[col_name] = [random.randint(1, 100) for _ in range(num_rows)]
            elif isinstance(column.type, Float):
                data[col_name] = [round(random.uniform(1, 100), 2) for _ in range(num_rows)]
            elif isinstance(column.type, Boolean):
                data[col_name] = [random.choice([True, False]) for _ in range(num_rows)]
            else:
                raise ValueError(f"Unsupported data type {column.data_type}")
        return pd.DataFrame(data)

    @staticmethod
    def generate_columns(num_cols: int) -> List[Column]:
        """
        Generate a list of columns with names generated using MD5 hash function.

        Args:
            num_cols (int): The number of columns to generate.

        Returns:
            List[Column]: A list of sqlalchemy Column objects.
        """
        columns = []
        for i in range(num_cols):
            col_name = hashlib.md5(str(i).encode()).hexdigest()
            col_type = String
            column = Column(col_name, col_type)
            columns.append(column)
        return columns

    @staticmethod
    def create_astro_table_object(name: str, metadata: Metadata, columns: List[Column]) -> Table:
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

    def conn_to_database(self, conn_id: str) -> BaseDatabase:
        """
        Connect to a database based on the provided connection ID.

        Args:
            conn_id (str): The connection ID to the database.

        Returns:
            BaseDatabase: An instance of a database connection class (e.g., PostgresDatabase).
        """
        assert self.db == 'postgres', 'Only Postgres is supported'
        return PostgresDatabase(conn_id=conn_id)

    def generate_tables(self, num_cols: int, num_rows: int, num_tables: int = None, name_tables: List[str] = None) -> List[Table]:
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
            columns = self.generate_columns(num_cols)
            table = self.create_astro_table_object(table_name, metadata, columns)
            return table

        if name_tables:
            # If a list of names is provided, create tables with those names and load them into the database
            for name in name_tables:
                table = create_table(name, num_cols, self.metadata)
                tables.append(table)
        else:
            # Otherwise, create tables with default names
            if num_tables is None:
                raise ValueError("You must specify the number of tables to generate.")
            
            # Otherwise, create tables with default names and load them into the database
            for i in range(num_tables):
                table_name = f'test_table_{i+1}'
                table = create_table(table_name, num_rows, num_cols, self.metadata)
                tables.append(table)
        
        return tables

    def append_tables_to_db(self, create_target_table_without_data: bool = True):
        """
        Append generated tables to the source and target database.

        Args:
            create_target_table_without_data (bool): Whether to create the target table without data (default is True).
        """
        # Initialize database connections
        source_db = self.conn_to_database(self.conn_id).sqlalchemy_engine
        target_db = self.conn_to_database(self.dest_conn_id).sqlalchemy_engine
        
        # Open connections
        with source_db.connect() as src_conn, target_db.connect() as tgt_conn:
            # Loop over tables
            for table in self.tables:
                # Generate data for table
                data = self.generate_random_data(self.num_rows, table.columns)

                # Load data into source table
                data.to_sql(name=table.name, con=src_conn, if_exists='replace', index=False)
                
                # Create target table if necessary
                if create_target_table_without_data:
                    # Create target table using sqlalchemy table object
                    sqla_table(f'{table.name}', MetaData(schema=self.metadata.schema), *table.columns).create(tgt_conn)
                else:
                    # Create target table using dataframe structure
                    data.to_sql(name=table.name, con=tgt_conn, if_exists='replace', index=False, method='multi')

    def create_tables(self, tables: List[Table], conn: PostgresDatabase = None) -> List[Table]:
        """
        Create tables in the database.

        Args:
            tables (List[Table]): A list of astro Table objects to create.
            conn (PostgresDatabase, optional): A specific database connection (default is None).

        Returns:
            List[Table]: A list of created astro Table objects.
        """
        if not conn:
            # Connect to PostgreSQL database using connection ID
            conn = self.conn_to_database(self.conn_id)

        # Create each table in the database
        for table in tables:
            conn.create_table(table)

        return tables

    def drop_tables(self, tables: Union[List[str], List[Table]]) -> List[str]:
        """
        Drop specified tables from the database.

        Args:
            tables (Union[List[str], List[Table]]): A list of table names or astro Table objects to drop.

        Returns:
            List[str]: A list of dropped table names.
        """
        # Connect to PostgreSQL database using connection ID
        db = self.conn_to_database(self.conn_id)

        dropped_tables = []

        for item in tables:
            # Check if item is a Table object
            if isinstance(item, Table):
                db.drop_table(item)
                dropped_tables.append(item.name)
            # Check if item is a string representing table name
            elif isinstance(item, str):
                db.execute(f"DROP TABLE IF EXISTS {item}")
                dropped_tables.append(item)
            else:
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
        db = self.conn_to_database(self.conn_id)
        
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
        db = self.conn_to_database(self.conn_id)
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
        db = self.conn_to_database(self.conn_id)
        for table_name in table_names:
            query = f"TRUNCATE TABLE {table_name}"
            db.run_sql(query)

    def __enter__(self):
        self.tables = self.generate_tables(self.num_cols, self.num_rows, num_tables=self.num_tables, name_tables=self.name_tables)
        
        return self

    def __exit__(self, type, value, traceback):
        pass