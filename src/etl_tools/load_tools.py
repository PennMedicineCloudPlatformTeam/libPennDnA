import os
import logging
import time
from databricks.sdk.runtime import *

from typing import List, Dict, Optional
from datetime import datetime
from dotenv import find_dotenv, load_dotenv
from dataclasses import dataclass, field, fields, asdict, InitVar
from pyspark.sql import DataFrameReader
from pyspark.sql import readwriter
from itertools import count


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit(sep='/')[-1])

if os.path.isfile(".env"):
    load_dotenv(find_dotenv())

    log_level = os.environ.get('LOGLEVEL', 'INFO').upper()
    logger.setLevel(level=log_level)
    logger.info("Set level to: " + log_level)
    target_catalog = os.environ.get('TARGET_CATALOG')
    target_schema = os.environ.get('TARGET_SCHEMA')
else:
    print("Please create your environment .env file!")
    exit(1)

start_time = time.time()

def fullTableParellelExtract(table_name, candidate_column=None, dbr_schema=None, src_server=None, src_database=None, src_schema=None, row_audit="N", new_extract="N"):

    dbr_schema = databricks_schema if dbr_schema in [None, ""] else dbr_schema
    src_server = source_server if src_server in [None, ""] else src_server
    src_database = source_database if src_database in [None, ""] else src_database
    src_schema = source_schema if src_schema in [None, ""] else src_schema
    source_url = f"jdbc:sqlserver://;serverName={src_server};databaseName={src_database}"

    sourceTableName = f"{src_schema}.{table_name}"
    targetTableName = f"{dbr_schema.lower()}.{table_name.lower()}"

    functions_logger.info(f"Table:{table_name}, Databricks Schema:{dbr_schema}, Source[ Server:{src_server}, DB:{src_database}, Schema:{src_schema} ]")

    pk_1 = candidate_column
    functions_logger.info(f"Candidate Column:{pk_1}")



    # ---------------------------------------------------------------------------------
    # Use the primary key to find bounds
    if pk_1 != None:

        bounds_query = f"""
        (SELECT MIN({pk_1}) MIN, MAX({pk_1}) MAX
        FROM [{table_name}]) AS MINMAX
        """

        startTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)

        bounds = spark.read \
            .format(source_format) \
            .option("url", source_url) \
            .option("TrustServerCertificate", "true") \
            .option("dbtable", bounds_query) \
            .option("user", source_jdbcUsername) \
            .option("password", source_jdbcPassword) \
            .load().collect()[0]
        run_parallel_extract = 1

        endTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)

        #Test data type
        if isinstance(bounds.MIN, numbers.Number):
            functions_logger.info(f"  Primary Key is a number, {pk_1} data type:" + str(type(bounds.MIN)))
            min_bounds = int(bounds.MIN)
            max_bounds = int(bounds.MAX)
            functions_logger.info(f"  Converted to INT, MIN:{min_bounds}, MAX:{max_bounds} [{endTS-startTS}]")
        elif isinstance(bounds.MIN, datetime):
            functions_logger.info(f"  Primary Key is a date, {pk_1} data type:" + str(type(bounds.MIN)))
            min_bounds = bounds.MIN.strftime('%Y-%m-%d')
            max_bounds = bounds.MAX.strftime('%Y-%m-%d')
            functions_logger.info(f"  Formatted Date, MIN:{min_bounds}, MAX:{max_bounds} [{endTS-startTS}]")
        else:
            functions_logger.info(f"  Primary Key is not a valid data type, must be a number or date, {pk_1} data type:{str(type(bounds.MIN))} [{endTS-startTS}]")
            run_parallel_extract = 0
    else:
        functions_logger.info(f"Candidate Column for table {table_name} not provided")



    # ----------------------------------------------------------------------------------
    # Systems go... 3, 2, 1... launch
    if run_parallel_extract == 1:
        source_df = spark.read \
            .format(source_format) \
            .option("url", source_url) \
            .option("TrustServerCertificate", "true") \
            .option("partitionColumn", pk_1) \
            .option("numPartitions", num_partitions) \
            .option("lowerBound", min_bounds) \
            .option("upperBound", max_bounds) \
            .option("dbtable", f"[{table_name}]") \
            .option("user", source_jdbcUsername) \
            .option("password", source_jdbcPassword) \
            .load()


        #Drop exisiting table for completely new extract
        if new_extract.upper() == "Y":
            startTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
            functions_logger.info(f"[{startTS.strftime('%I:%M:%S %p')}] Dropping existing table... ", end="")

            try:
                spark.sql(f"DROP TABLE IF EXISTS {targetTableName}")
                exitStatus = "Done"
            except Exception as e:
                exitStatus = f"{e}"

            endTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
            functions_logger.info(f"{exitStatus} [{endTS-startTS}]")


        #Start table write
        startWriteTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"[{startWriteTS.strftime('%I:%M:%S %p')}] Reading and loading table: {targetTableName}... ", end="")

        source_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{targetTableName}")

        endWriteTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"Done [{endWriteTS-startWriteTS}]")



    # ----------------------------------------------------------------------------------
    # Row audit
    if row_audit.upper() == "Y":
        source_row_count_query = f"""
        SELECT COUNT(*) AS ROW_COUNT
        FROM [{sourceTableName}]
        """

        startTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"[{startTS.strftime('%I:%M:%S %p')}] Starting row count audit... ", end="")

        source_row_count = spark.read \
            .format(source_format) \
            .option("url", source_url) \
            .option("TrustServerCertificate", "true") \
            .option("query", source_row_count_query) \
            .option("user", source_jdbcUsername) \
            .option("password", source_jdbcPassword) \
            .load().first()['ROW_COUNT']

        destination_row_count = spark.read.table(targetTableName).count()

        endTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"Source row count:{'{:,}'.format(source_row_count)}, Destination row count:{'{:,}'.format(destination_row_count)}, Difference:{(source_row_count - destination_row_count)} [{endTS-startTS}]")


def fullTableSequentialExtract(table_name, dbr_schema=None, src_server=None, src_database=None, src_schema=None, row_audit="N", new_extract="N"):

    dbr_schema = databricks_schema if dbr_schema in [None, ""] else dbr_schema
    src_server = source_server if src_server in [None, ""] else src_server
    src_database = source_database if src_database in [None, ""] else src_database
    src_schema = source_schema if src_schema in [None, ""] else src_schema
    source_url = f"jdbc:sqlserver://;serverName={src_server};databaseName={src_database}"

    sourceTableName = f"{src_schema}.{table_name}"
    targetTableName = f"{dbr_schema.lower()}.{table_name.lower()}"

    functions_logger.info(f"Table:{table_name}, Databricks Schema:{dbr_schema}, Source[ Server:{src_server}, DB:{src_database}, Schema:{src_schema} ]")


    # ----------------------------------------------------------------------------------
    # Drop exisiting table for completely new extract
    if new_extract.upper() == "Y":
        startTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"[{startTS.strftime('%I:%M:%S %p')}] Dropping existing table... ", end="")

        try:
            spark.sql(f"DROP TABLE IF EXISTS {targetTableName}")
            exitStatus = "Done"
        except Exception as e:
            exitStatus = f"{e}"

        endTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"{exitStatus} [{endTS-startTS}]")



    # ----------------------------------------------------------------------------------
    # Read and then write the source table to target table
    try:
        startWriteTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        print(f"[{startWriteTS.strftime('%I:%M:%S %p')}] loading table {targetTableName.lower()} from {sourceTableName.lower()}... ", end="")

        spark.read \
            .format(source_format) \
            .option("url", source_url) \
            .option("TrustServerCertificate", "true") \
            .option("dbtable", f"[{table_name}]") \
            .option("user", source_jdbcUsername) \
            .option("password", source_jdbcPassword) \
            .load() \
            .write.mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(targetTableName)

        exitStatus = "Done"
    except Exception as e:
        exitStatus = f"{e}"

    endWriteTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
    functions_logger.info(f"{exitStatus} [{endWriteTS-startWriteTS}]")



    # ----------------------------------------------------------------------------------
    # Row audit
    if row_audit.upper() == "Y":
        source_row_count_query = f"""
        SELECT COUNT(*) AS ROW_COUNT
        FROM [{table_name}]
        """

        startTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"[{startTS.strftime('%I:%M:%S %p')}] Starting row count audit... ", end="")

        source_row_count = spark.read \
            .format(source_format) \
            .option("url", source_url) \
            .option("TrustServerCertificate", "true") \
            .option("query", source_row_count_query) \
            .option("user", source_jdbcUsername) \
            .option("password", source_jdbcPassword) \
            .load().first()['ROW_COUNT']

        destination_row_count = spark.read.table(targetTableName).count()

        endTS = datetime.now().astimezone(est).replace(tzinfo=None).replace(microsecond=0)
        functions_logger.info(f"Source row count:{'{:,}'.format(source_row_count)}, \
                               Destination row count:{'{:,}'.format(destination_row_count)}, \
                               Difference:{(source_row_count - destination_row_count)} [{endTS-startTS}]")


def get_pk_column(*, table_name: str, conn: readwriter.DataFrameReader) -> str:
    pk_query = '(SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = \'PRIMARY KEY\' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND KU.table_name = \'' + table_name + '\') as pk_query'

    pks = conn(pk_query).load().collect()[0]

    return pks.PRIMARYKEYCOLUMN


def get_table_bounds(table_item: dict, conn: readwriter.DataFrameReader) -> List[str]:
    bounds_query = "(SELECT MIN({pk}) minkey, MAX({pk}) maxkey FROM {table}) AS MINMAX".format(pk=table_item['primaryKey'], table=table_item['tableName'])

    bounds = conn(bounds_query).load().collect()[0]

    logger.debug("Min: {}, Max: {}".format(bounds['minkey'], bounds['maxkey']))

    return bounds


def test_function(n: str = "something") -> None:
    functions_logger.warning("This is a warning message")
    functions_logger.error("This is an error message")
    functions_logger.debug("About to do something")
    functions_logger.info("This is an informational message")


@dataclass
class Metadata:
    '''
    Purely data datastructure
    '''
    identifier: int = field(default_factory=count().__next__, init=False)
    source_table_name: str
    source_schema_name: str
    source_primary_key: Optional[str] = field(default=None)
    source_min_pk_val: Optional[int] = field(default=None)
    source_max_pk_val: Optional[int] = field(default=None)
    target_catalog_name: Optional[str] = field(default=None)
    target_schema_name: Optional[str] = field(default=None)
    target_table_name: Optional[str] = field(default=None)
    target_min_pk_val: Optional[int] = field(default=None)
    target_max_pk_val: Optional[int] = field(default=None)
    process_start_time: Optional[datetime] = field(default=None)
    process_end_time: Optional[datetime] = field(default=None)

    def __iter__(self):
        return self.fields()


@dataclass(kw_only=True)
class Tables:
    """
        Table container datastructure

        Example:
        prepopulate = ["Procedures", "Series", "Images"]
        tables = Tables(init_table=prepopulate)

        Attributes
        ----------
        init_table : list
            Optional list can be fed into the class instantiation to prepopulate the datastructure
        table_list : dict
            Dictionary of Metadata class objects

        Methods
        -------
        get_item(identifier=int)
            Returns Metadata class object

        add_item(metadata=Metadata)
            Returns an integer upon successful addition to the structure

    """

    init_table: InitVar[List] = []
    table_list: Dict[int, Metadata] = field(default_factory=dict, metadata="List of tables")
    _indexes: List[int] = field(default_factory=list)

    # Singleton table instances
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

    def __iter__(self):
        return iter(self.table_list.items())
        # for field in dataclasses.fields(self):
        #     yield getattr(self, field.name)

    def __next__(self):
        idx = self._iter_index
        if idx is None or idx >= len(self.container):
            self._iter_index = None
            raise StopIteration()
        value = list(self.container)[idx]
        self._iter_index = idx + 1
        return value

    def __len__(self):
        return len(self.table_list)

    def __getitem__(self, sliced):
        if isinstance(sliced, slice):
            slicedkeys = self._indexes[sliced]
            return {key: self.table_list[key] for key in slicedkeys}
        return self.table_list

    # If instantiated with a list, create a list of table names from it
    def __post_init__(self, init_table):

        _instance = None
        _iter_index = 0

        if os.path.isfile(".env"):
            load_dotenv(find_dotenv())
            self.source_database = os.getenv("SOURCE_DATABASE")
            self.source_schema = os.getenv("SOURCE_SCHEMA")
            self.target_catalog = os.getenv("TARGET_CATALOG")
            self.target_schema = os.getenv("TARGET_SCHEMA")
        if len(init_table) > 0:
            for table in init_table:
                self.add_item(Metadata(source_table_name=table,
                                       source_schema_name=self.source_database,
                                       target_catalog_name = self.target_catalog,
                                       target_schema_name = self.target_schema,
                                       target_table_name = table
                                       ))

    def get_item(self, identifier: int) -> Metadata:
        try:
            return self.table_list[identifier]
        except Exception as e:
            print(e)

    def add_item(self, Metadata) -> int:
        identifier: int = Metadata.identifier
        try:
            self.table_list[identifier] = Metadata
            self._indexes.append(identifier)
            return identifier
        except Exception as e:
            print(e)
            return -1

    def getids(self) -> list[int]:
        return _indexes

    # def __iter__(self):
    #     for field in fields(self):
    #         yield getattr(self, field.name)


class DBConnection:
    '''
    Singleton Database connection

    Example:
        conn = DBConnection()
        example_query = "(SELECT value FROM table) AS alias_name"
        bounds = conn(bounds_query).load().collect()

    '''
    _instance = None

    # Singleton only database connections
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if os.path.isfile(".env"):
            load_dotenv(find_dotenv())
            self.source_server = os.getenv("SOURCE_SERVER")
            self.source_database = os.getenv("SOURCE_DATABASE")
            self.source_secret_scope = os.getenv("SOURCE_SECRET_SCOPE")
            self.source_secret_user = os.getenv("SOURCE_SECRET_USER")
            self.source_secret_pass = os.getenv("SOURCE_SECRET_PASS")
        else:
            raise

    def __call__(self, query) -> DataFrameReader:

        return spark.read.format("jdbc") \
            .option("url", "jdbc:sqlserver://;serverName={};databaseName={}".format(self.source_server, self.source_database)) \
            .option("TrustServerCertificate", "true") \
            .option("user", dbutils.secrets.get(scope=self.source_secret_scope, key=self.source_secret_user)) \
            .option("password", dbutils.secrets.get(scope=self.source_secret_scope, key=self.source_secret_pass)) \
            .option("dbtable", query)
