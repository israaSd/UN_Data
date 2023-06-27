
# %%
import hashlib
import datetime
from pydantic.typing import Annotated
from pydantic import Field
import logging
import pyodbc
import configparser
import os
import traceback2 as traceback

# %%
current_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = os.path.join(current_dir, 'config.ini')
config = configparser.ConfigParser()
config.read(config_file_path)
server = config['DataMigrationAzureDB']['server']
database = config['DataMigrationAzureDB']['database']
username = config['DataMigrationAzureDB']['username']
password = config['DataMigrationAzureDB']['password']
driver = config['DataMigrationAzureDB']['driver']


# %%
def hash_file(path):
    """
        Input: Path to the downloaded file
        Output: Hash value of the downloaded file
    """
    with open(path, 'rb') as f:
        data = f.read()
    hash_value = hashlib.sha256(data).hexdigest()
    return hash_value


def _hashing(
        source: Annotated[
            str,
            Field(
                title="Source",
                description='Source of the scraped data',
                example="UNESCO"

            )],

        tablename: Annotated[
            str,
            Field(
                title="Table Name",
                description='Name of the table that will be added to the inferschema json',
                example="Total R&D personnel (HC) - Female"

            )],

        path_scraped_file: Annotated[
            str,
            Field(
                title="Path to Scraped File",
                description='The path of where the data to be migrated is downloaed. This will be used for hashing data',
                example="\\10.30.31.77\data_collection_dump\RawData\testsource\test1.csv"

            )]
):
    """
        Input: Source of the data, table name that will be sent to the infer schema and the path to the downloaded file.
        Output: Json that indicates if Infer Schema should be triggered or not
        If Success is true, then the function was successful, else add it to the logs by accessing the "message" key
        Example:{"Success": True,
                "Trigger_InferSchema":True,   
                "message" : f"{tablename} from {source} should be updated"}
    """
    try:
        conn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};")
        source = source.lower()
        with  conn.cursor() as cursor:
            cursor.execute(f"""Select Top 1 * from  [dbo].[HashValues]
                        where Source ='{source}'
                        and TableName = '{tablename}' 
                        ORDER BY Last_Updated_Hash DESC;""")
            res = cursor.fetchall()
            new_hash = hash_file(path_scraped_file)
            if len(res) == 0:
                cursor.execute(f"""INSERT INTO [dbo].[HashValues]
                                (Source, TableName, Hash, Last_Updated_Hash,FileName,hash_path)
                                VALUES ('{source}',
                                '{tablename}',
                                '{new_hash}',
                                '{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                                '{path_scraped_file}',
                                '')
                                """)
                return {"Success": True,
                        "Trigger_InferSchema": True,
                        "message": f"{tablename} from {source} should be updated."}

            elif res[0][2] == new_hash:
                return {
                    "Success": True,
                    "Trigger_InferSchema": False,
                    "message": f"{tablename} from  {source} is still the same, no update needed"}
            else:
                cursor.execute(f"""INSERT INTO [dbo].[HashValues]
                            (Source, TableName, Hash, Last_Updated_Hash,FileName,hash_path)
                            VALUES ('{source}',
                                '{tablename}',
                                '{new_hash}',
                                '{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                                '{path_scraped_file}',
                                '')
                                """)
                return {
                    "Success": True,
                    "Trigger_InferSchema": True,
                    "message": f"{tablename} from {source} should be updated."}
    except:
        return {
            "Success": False,
            "message": traceback.format_exc()
        }
# %%
