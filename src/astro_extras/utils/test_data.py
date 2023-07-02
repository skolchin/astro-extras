import random
import string
import hashlib
import pandas as pd

from typing import List, Union
from astro.sql.table import Table, Metadata
from astro.databases.postgres import PostgresDatabase
from sqlalchemy import Boolean, Column, Float, MetaData, String, Integer

class TestData:
    # Class for generating test data and creating tables in a database using astro and sqlalchemy
    def __init__(self, conn_id: str) -> None:
        # Initialize metadata and connection ID
        self.conn_id: str = conn_id
        self.metadata: MetaData = Metadata(schema='public')

    @staticmethod
    def generate_random_data(num_rows: int, columns: List[Column]) -> pd.DataFrame:
        # Generate random data for each column in a DataFrame
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
    def create_astro_table_object(name: str, metadata: Metadata, columns: List[Column]) -> Table:
        # Create a new astro table object with the given name, metadata and columns
        return Table(name=name, metadata=metadata, columns=columns)

    @staticmethod
    def generate_columns(num_cols: int) -> List[Column]:
        # Generate a list of columns with names generated using MD5 hash function
        columns = []
        for i in range(num_cols):
            col_name = hashlib.md5(str(i).encode()).hexdigest()
            col_type = String
            column = Column(col_name, col_type)
            columns.append(column)
        return columns

    @staticmethod
    def conn_to_postrges(conn_id: str) -> PostgresDatabase:
        # Create and return a new PostgresDatabase object with the given connection ID
        return PostgresDatabase(conn_id=conn_id)

    def generate_tables_and_uppend_to_db(self, num_cols: int, num_rows: int, num_tables: int = None, name_tables: List[str]=None) -> List[str]:
        tables = []

        def create_and_load_table(table_name: str, num_rows: int, num_cols: int, metadata, conn_id):
            # Create a new table with the given name, metadata and columns, load it into the database
            columns = self.generate_columns(num_cols)
            table = self.create_astro_table_object(table_name, metadata, columns)
            db = self.conn_to_postrges(conn_id)

            # Generate data for the table and load it into the database
            data = self.generate_random_data(num_rows, columns)
            db.load_pandas_dataframe_to_table(source_dataframe=data,
                                            target_table=table,
                                            if_exists='replace')
            return table

        if name_tables:
            # If a list of names is provided, create tables with those names and load them into the database
            for name in name_tables:
                table_name = name
                table = create_and_load_table(table_name, num_rows, num_cols, self.metadata, self.conn_id)
                tables.append(table)
        else:
            # Otherwise, create tables with default names and load them into the database
            for i in range(num_tables):
                table_name = f'test_table_{i+1}'
                table = create_and_load_table(table_name, num_rows, num_cols, self.metadata, self.conn_id)
                tables.append(table)

        return tables

    def serialize_tables(self, tables: List[Table]) -> List[dict]:
        # Serialize a list of tables into a list of dictionaries
        serial_tables = []

        for table in tables:
            serialized_data = {
                'name': table.name,
                'metadata': {
                    'schema': table.metadata.schema,
                },
                'columns': [
                    {
                        'name': col.name,
                        'type': str(col.type),
                    }
                    for col in table.columns
                ],
                'temp': table.temp,
            }
            serial_tables.append(serialized_data)
        return serial_tables
    
    def deserialize_tables(self, serialized_data: List[dict]) -> List[dict]:
        # Check if serialized_data is a list
        if not isinstance(serialized_data, list):
            raise TypeError('Serialized data must be a dictionary')

        deserial_tables = []

        for table in serialized_data:
            # Create metadata object from serialized data.
            metadata = MetaData(schema=str(table['metadata']['schema']))

            # Create column objects from serialized data.
            columns = [ 
                Column(col_dict['name'], String) for col_dict in table['columns']
            ]

            # Append deserialized table to the list
            deserial_tables.append({'name':str(table['name']), 'metadata':metadata, 'columns':columns})

        return deserial_tables

    def create_tables(self, tables: List[Table]) -> List[Table]:
        # Connect to PostgreSQL database using connection ID
        db = self.conn_to_postrges(self.conn_id)

        # Create each table in the database
        for table in tables:
            db.create_table(table)

        return tables
    
    def drop_tables(self, tables: Union[List[str], List[Table]]) -> List[str]:
        # Connect to PostgreSQL database using connection ID
        db = self.conn_to_postrges(self.conn_id)

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
    
    def get_table_data(self, table_name: str) -> pd.DataFrame:
        # Retrieve all data from a specific table and return it as a Pandas DataFrame.
        db = self.conn_to_postrges(self.conn_id)
        query = f"SELECT * FROM {table_name}"
        data = db.run_sql(query)
        return pd.DataFrame(data)

    def get_table_names(self) -> List[str]:
        # Retrieve the names of all tables in the 'public' schema of the database.
        db = self.conn_to_postrges(self.conn_id)
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        result = db.run_sql(query)
        table_names = [row[0] for row in result]
        return table_names

    def truncate_tables(self, table_names: List[str]) -> None:
        # Truncate (remove all rows) from the specified tables.
        db = self.conn_to_postrges(self.conn_id)
        for table_name in table_names:
            query = f"TRUNCATE TABLE {table_name}"
            db.run_sql(query)

