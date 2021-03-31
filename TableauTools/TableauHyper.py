# -----------------------------------------------------------------------------
#
# 使用Tableau Hyper API 创建数据源
# 20210327经过测试发现，直接使用 hyper-sql的COPY命令，600万行30列CSV，转hyper只需要17秒
# 这里将其打包成函数以便于使用
# 优点，速度非常快，缺点，COPY命令遇错即停，仅告知行号，错误需要自己定位
# 因此有可能需要异常处理方案，例如出错的列用string
# -----------------------------------------------------------------------------
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException

import pandas as pd
import numpy as np
from tqdm import tqdm
import tableauhyperapi as tab_api
from typing import Dict, List, Optional, Sequence, Tuple, Union
import datetime
import collections
from distutils.version import LooseVersion

# for data source publish
import tableauserverclient as TSC

# 检查 pandas 是否 1.0版本及以上，对应的列类型关系不同
PANDAS_100 = LooseVersion(pd.__version__) >= LooseVersion("1.0.0")

# The Hyper API as of writing doesn't offer great hashability for column comparison
# so we create out namedtuple for that purpose
_ColumnType = collections.namedtuple("_ColumnType", ["type_", "nullability"])

TableType = Union[str, tab_api.Name, tab_api.TableName]

_column_types = {
    "int16": _ColumnType(tab_api.SqlType.small_int(), tab_api.Nullability.NOT_NULLABLE),
    "int32": _ColumnType(tab_api.SqlType.int(), tab_api.Nullability.NOT_NULLABLE),
    "int64": _ColumnType(tab_api.SqlType.big_int(), tab_api.Nullability.NOT_NULLABLE),
    "Int16": _ColumnType(tab_api.SqlType.small_int(), tab_api.Nullability.NULLABLE),
    "Int32": _ColumnType(tab_api.SqlType.int(), tab_api.Nullability.NULLABLE),
    "Int64": _ColumnType(tab_api.SqlType.big_int(), tab_api.Nullability.NULLABLE),
    "float32": _ColumnType(tab_api.SqlType.double(), tab_api.Nullability.NULLABLE),
    "float64": _ColumnType(tab_api.SqlType.double(), tab_api.Nullability.NULLABLE),
    "bool": _ColumnType(tab_api.SqlType.bool(), tab_api.Nullability.NOT_NULLABLE),
    "datetime64[ns]": _ColumnType(
        tab_api.SqlType.timestamp(), tab_api.Nullability.NULLABLE
    ),
    "datetime64[ns, UTC]": _ColumnType(
        tab_api.SqlType.timestamp_tz(), tab_api.Nullability.NULLABLE
    ),
    "timedelta64[ns]": _ColumnType(
        tab_api.SqlType.interval(), tab_api.Nullability.NULLABLE
    ),
    "object": _ColumnType(tab_api.SqlType.text(), tab_api.Nullability.NULLABLE),
}

if PANDAS_100:
    _column_types["string"] = _ColumnType(
        tab_api.SqlType.text(), tab_api.Nullability.NULLABLE
    )
    _column_types["boolean"] = _ColumnType(
        tab_api.SqlType.bool(), tab_api.Nullability.NULLABLE
    )
else:
    _column_types["object"] = _ColumnType(
        tab_api.SqlType.text(), tab_api.Nullability.NULLABLE
    )


# Invert this, but exclude float32 as that does not roundtrip
_pandas_types = {v: k for k, v in _column_types.items() if k != "float32"}

# Add things that we can't write to Hyper but can read
_pandas_types[
    _ColumnType(tab_api.SqlType.date(), tab_api.Nullability.NULLABLE)
] = "date"
if PANDAS_100:
    _pandas_types[
        _ColumnType(tab_api.SqlType.text(), tab_api.Nullability.NOT_NULLABLE)
    ] = "string"
else:
    _pandas_types[
        _ColumnType(tab_api.SqlType.text(), tab_api.Nullability.NOT_NULLABLE)
    ] = "object"


def _pandas_to_tableau_type(typ: str) -> _ColumnType:
    try:
        return _column_types[typ]
    except KeyError:
        raise TypeError("Conversion of '{}' dtypes not supported!".format(typ))

"""
Dask相当不好用，各种手动指定列的类型，类型不统一报错。写出csv比pandas还要慢
Dask似乎只是给pandas加上Lazyeval，并没有提速效果
如果用 Modin 是否可以提速，需要验证
也许可以试试data.table在python中对应的库
"""

def buildTabDefination(df, hyper_table_name='Extract'):
    """
    使用 Pandas dataframe创建hyper的表结构
    """

    table_def = tab_api.TableDefinition(hyper_table_name)

    # Populate insertion mechanisms dependent on column types
    column_types: List[_ColumnType] = []
    columns: List[tab_api.TableDefinition.Column] = []
    for col_name, dtype in df.dtypes.items():
        column_type = _pandas_to_tableau_type(dtype.name)
        column_types.append(column_type)
        columns.append(
            tab_api.TableDefinition.Column(
                name=col_name,
                type=column_type.type_,
                nullability=column_type.nullability,
            )
        )

    for column, column_type in zip(columns, column_types):
        table_def.add_column(column)

    return table_def

def csv2hyper(table_definition, path_to_csv, path_to_hyper):
    """
    An example demonstrating loading data from a csv into a new Hyper file
    For more details, see https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_insert_csv.html
    """
    print("EXAMPLE - Load data from CSV into table in new Hyper file")

    # path_to_database = Path("customer.hyper")
    path_to_database = Path(path_to_hyper)

    # Optional process parameters.
    # They are documented in the Tableau Hyper documentation, chapter "Process Settings"
    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
    process_parameters = {
        # Limits the number of Hyper event log files to two.
        "log_file_max_count": "2",
        # Limits the size of Hyper event log files to 100 megabytes.
        "log_file_size_limit": "100M"
    }

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:

        # Optional connection parameters.
        # They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
        # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
        connection_parameters = {"lc_time": "en_US"}

        # Creates new Hyper file "customer.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        # CreateMode目前并没有什么好选的，直接创建新的替换会更好
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE, 
                        parameters=connection_parameters) as connection:

            connection.catalog.create_table(table_definition=table_definition)

            # Using path to current file, create a path that locates CSV file packaged with these examples.
            # path_to_csv = str(Path(__file__).parent / "data" / "customers.csv")


            # Load all rows into "Customers" table from the CSV file.
            # `execute_command` executes a SQL statement and returns the impacted row count.
            #
            # Note:
            # You might have to adjust the COPY parameters to the format of your specific csv file.
            # The example assumes that your columns are separated with the ',' character
            # and that NULL values are encoded via the string 'NULL'.
            # Also be aware that the `header` option is used in this example:
            # It treats the first line of the csv file as a header and does not import it.
            #
            # The parameters of the COPY command are documented in the Tableau Hyper SQL documentation
            # (https:#help.tableau.com/current/api/hyper_api/en-us/reference/sql/sql-copy.html).
            print("Issuing the SQL COPY command to load the csv file into the table. Since the first line")
            print("of our csv file contains the column names, we use the `header` option to skip it.")
            count_in_customer_table = connection.execute_command(
                command=f"COPY {table_definition.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL '', delimiter ',', header)")

            print(f"The number of rows in table {table_definition.table_name} is {count_in_customer_table}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


def publishDataSource(host, username, password, projectName, datasourceFilePath, datasourceName=None):
    """
    Shows how to leverage the Tableau Server Client (TSC) to sign in and publish an extract directly to Tableau Online/Server
    datasourceName中，不加.hyper后缀 加了之后会导致发布的文件重复的 .hyper后缀
    """

    server_address = host

    # Sign in to server
    tableau_auth = TSC.TableauAuth(username, password)
    server = TSC.Server(server_address, use_server_version=True)
    
    print(f"Signing into {server_address}")
    with server.auth.sign_in(tableau_auth):
        # Define publish mode - Overwrite, Append, or CreateNew
        publish_mode = TSC.Server.PublishMode.Overwrite
        
        # Get project_id from project_name
        # all_projects, pagination_item = server.projects.get()
        for project in TSC.Pager(server.projects):
            if project.name == projectName:
                project_id = project.id
    
        # Create the datasource object with the project_id
        datasource = TSC.DatasourceItem(project_id, datasourceName)
        
        print(f"Publishing {datasourceName} to {projectName}...")
        # Publish datasource
        datasource = server.datasources.publish(datasource, datasourceFilePath, publish_mode)
        print("Datasource published. Datasource ID: {0}".format(datasource.id))




if __name__ == '__main__':
    
    time_start = datetime.datetime.now()
    print("Starts: %s" % (time_start))
    # try:
    #     run_create_hyper_file_from_csv('data/realtime_cost.test_dask.csv')
    # except HyperException as ex:
    #     print(ex)
    #     exit(1)
    
    hyper_path = 'test_data/test.hyper'
    csv_path = 'test_data/test_for_hyper.csv'
    # df = pd.read_csv(csv_path, nrows=10000)
    sample_df_nrows = 10000
    df = pd.DataFrame(
        dict(
            id=range(sample_df_nrows), 
            value=np.random.randn(sample_df_nrows)
            )
        )
    df.to_csv(csv_path, index=False)
    
    try:
        table_def = buildTabDefination(df, 'RandomValues')
        csv2hyper(table_def, csv_path, hyper_path)
    except HyperException as ex:
        print(ex)
        

    time_end = datetime.datetime.now()
    print("Ends: %s" % (time_end))
    print("Costs: %s" % (time_end - time_start))
