import base64
import json
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from pyspark import sql
from connector.pg_connect import *
from utils.logger import Logger

logger = Logger()


class IngestionAttr:
    def __init__(self, conn, config, args):
        try:
            self.conn = conn
            self.fm_prefix = config["fm_prefix"]
            self.ingestion_region = config["primary_region"]
            self.src_sys_id = args["source_id"]
            self.asset_id = args["asset_id"]
            self.exec_id = args["exec_id"]
            self.source_path = args["source_path"]
            src_sys_attr = self.get_src_sys_attributes(conn)
            self.ing_pattern = src_sys_attr["ingstn_pattern"]
            self.db_type = src_sys_attr["db_type"]
            self.db_hostname = src_sys_attr["db_hostname"]
            self.db_username = src_sys_attr["db_username"]
            self.db_schema = src_sys_attr["db_schema"]
            self.db_port = src_sys_attr["db_port"]
            self.db_name = src_sys_attr["db_name"]
            self.bucket_name = src_sys_attr["ingstn_src_bckt_nm"]
            asset_attr = self.get_data_asset_attributes(conn)
            self.table_name = asset_attr["src_table_name"]
            self.query = asset_attr["src_sql_query"]
            self.trigger_mechanism = asset_attr["trigger_mechanism"]
            self.ext_method = asset_attr["ext_method"]
            self.ext_col = asset_attr["ext_col"]
            self.password = self.get_secret() if self.ing_pattern == "database" else None
            self.timestamp = self.source_path.split("/")[5]
            self.driver = None
            self.url = None
            self.max_value_in_catalog = None
            self.max_value_in_table = None
        except Exception as e:
            logger.write(message=str(e))

    def get_src_sys_attributes(self, conn):

        src_sys_table = "source_system_ingstn_atrbts"
        src_sys_table_data = conn.retrieve_dict(table=src_sys_table, cols="all",
                                                where=("src_sys_id = %s", [self.src_sys_id]))
        print(src_sys_table_data[0])
        return src_sys_table_data[0]

    def get_data_asset_attributes(self, conn):
        data_asset_table = "data_asset_ingstn_atrbts"
        data_asset_table_data = conn.retrieve_dict(table=data_asset_table, cols="all",
                                                   where=("asset_id=%s", [self.asset_id]))
        print(data_asset_table_data[0])
        return data_asset_table_data[0]

    def get_data_catalog_attributes(self):
        data_asset_catalog_table = "data_asset_catalogs"
        data_asset_catalog_table_data = self.conn.retrieve_dict(table=data_asset_catalog_table, cols="all",
                                                                where=("asset_id=%s", [self.asset_id]))
        print(data_asset_catalog_table_data[0])
        return data_asset_catalog_table_data[0]

    def get_secret(self):
        secret_name = f"{self.fm_prefix}-ingstn-db-secrets-{self.src_sys_id}"
        region_name = self.ingestion_region

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                key_value_pair = json.loads(secret)
                password = key_value_pair[str(self.src_sys_id)]
                return password
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret

    def drop_data_to_s3(self, data):
        data.repartition(1).write.csv(self.source_path, header=True, mode="overwrite")

    def get_highest_value_from_catalog(self):
        data_asset_catalog_table = "data_asset_catalogs"
        data_asset_catalog_table_data = self.conn.retrieve_dict(table=data_asset_catalog_table, cols="last_ext_time",
                                                                where=("asset_id=%s and last_ext_time is not %s",
                                                                       [self.asset_id, None]),
                                                                order=["last_ext_time", "DESC"])
        if not data_asset_catalog_table_data:
            return None
        else:
            return data_asset_catalog_table_data[0]["last_ext_time"]

    def get_data_from_different_db(self, get_max=None, full=None, inc=None):
        if self.db_type == "postgres":
            self.driver = "org.postgresql.Driver"
            self.url = f"jdbc:postgresql://{self.db_hostname}:{self.db_port}/{self.db_name}"
            if get_max is True:
                self.query = f"select {self.ext_col} from {self.db_schema}.{self.table_name} ORDER BY {self.ext_col} DESC LIMIT 1"
            if full is True:
                self.query = f"SELECT * FROM {self.db_schema}.{self.table_name}"
            if inc is True:
                self.query = f"select * from {self.db_schema}.{self.table_name} where {self.ext_col} > timestamp '{self.max_value_in_catalog}' and {self.ext_col} <= timestamp '{self.max_value_in_table}'"
                print(self.query)
        if self.db_type == "mysql":
            self.driver = "com.mysql.jdbc.Driver"
            self.url = f"jdbc:mysql://{self.db_hostname}:{self.db_port}/{self.db_name}"
            if get_max is True:
                self.query = f"select {self.ext_col} from {self.table_name} ORDER BY {self.ext_col} DESC LIMIT 1"
            if full is True:
                self.query = f"SELECT * FROM {self.table_name}"
            if inc is True:
                self.query = f"select * from {self.table_name} where {self.ext_col} > timestamp {str(self.max_value_in_catalog)} and {self.ext_col} <= timestamp {str(self.max_value_in_table)}"
        if self.db_type == "oracle":
            self.driver = "oracle.jdbc.driver.OracleDriver"
            self.url = f"jdbc:oracle:thin:@{self.db_hostname}:{self.db_port}:{self.db_name}"
            if get_max is True:
                self.query = f"select {self.ext_col} from {self.table_name} ORDER BY {self.ext_col} DESC LIMIT 1"
            if full is True:
                self.query = f"SELECT * FROM {self.table_name}"
            if inc is True:
                self.query = f"select * from {self.table_name} where {self.ext_col} > timestamp {str(self.max_value_in_catalog)} and {self.ext_col} <= timestamp {str(self.max_value_in_table)}"
        if self.db_type == "sqlserver":
            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            self.url = f"jdbc:sqlserver://{self.db_hostname}:{self.db_hostname};Server={self.db_hostname};Database={self.db_name};Trusted_Connection=True"
            if get_max is True:
                self.query = f"select {self.ext_col} from {self.table_name} ORDER BY {self.ext_col} DESC LIMIT 1"
            if full is True:
                self.query = f"SELECT * FROM {self.table_name}"
            if inc is True:
                self.query = f"select * from {self.table_name} where {self.ext_col} > timestamp {str(self.max_value_in_catalog)} and {self.ext_col} <= timestamp {str(self.max_value_in_table)}"
        try:
            spark = sql.SparkSession.builder.getOrCreate()
            df = spark.read.format("jdbc").options(driver=self.driver,
                                                   user=self.db_username,
                                                   password=self.password,
                                                   url=self.url,
                                                   query=self.query
                                                   ).load()
            return df
        except Exception as e:
            logger.write(message=str(e))

    def pull_data_from_db(self):
        if self.ext_method == "incremental":
            time_df = self.get_data_from_different_db(get_max=True)
            self.max_value_in_table = time_df.collect()[0][0]
            print(self.max_value_in_table)
            time.sleep(5)
            self.max_value_in_catalog = self.get_highest_value_from_catalog()
            print(self.max_value_in_catalog)
            if self.max_value_in_catalog is None:
                return self.get_data_from_different_db(full=True)
            else:
                return self.get_data_from_different_db(inc=True)
        elif self.ext_method == "full":
            return self.get_data_from_different_db(full=True)

    def copy_file_between_buckets(self):
        if self.trigger_mechanism == "time_driven":
            ing_bucket = f"{self.fm_prefix}-time-drvn-inbound-{self.ingestion_region}"
        else:
            ing_bucket = f"{self.fm_prefix}-evnt-drvn-inbound-{self.ingestion_region}"
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(ing_bucket)
        try:
            for obj in my_bucket.objects.filter(Prefix=f"init/{self.src_sys_id}/{self.asset_id}/"):
                copy_source = {
                    'Bucket': ing_bucket,
                    'Key': obj.key
                }
                dest_bucket = s3.Bucket(self.bucket_name)
                file_name = obj.key.split("/")[3]
                dest_bucket.copy(copy_source, f"{self.asset_id}/init/{self.timestamp}/{file_name}")
        except Exception as e:
            logger.write(message=str(e))

    def move_file_within_bucket(self):
        if self.trigger_mechanism == "time_driven":
            ing_bucket = f"{self.fm_prefix}-time-drvn-inbound-{self.ingestion_region}"
        else:
            ing_bucket = f"{self.fm_prefix}-evnt-drvn-inbound-{self.ingestion_region}"
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(ing_bucket)
        try:
            for obj in my_bucket.objects.filter(Prefix=f"init/{self.src_sys_id}/{self.asset_id}/"):
                copy_source = {
                    'Bucket': ing_bucket,
                    'Key': obj.key
                }
                file_name = obj.key.split("/")[3]
                my_bucket.copy(copy_source, f"processed/{self.src_sys_id}/{self.asset_id}/{file_name}")
                s3.Object(obj.bucket_name, obj.key).delete()
        except Exception as e:
            logger.write(message=str(e))

    def insert_record_in_catalog_tbl(self):
        created_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            "exec_id": self.exec_id,
            "src_sys_id": int(self.src_sys_id),
            "asset_id": int(self.asset_id),
            "dq_validation": "not started",
            "data_publish": "not started",
            "data_masking": "not started",
            "src_file_path": self.source_path,
            "s3_log_path": f"s3://{self.bucket_name}/{self.asset_id}/logs/{self.exec_id}/",
            "proc_start_ts": datetime.strptime(self.timestamp, "%Y%m%d%H%M%S"),
            "created_ts": created_ts,
            "last_ext_time": self.max_value_in_table
        }
        self.conn.insert(table="data_asset_catalogs", data=insert_data)

    def merge_and_copy_streaming_file_to_raw(self):
        s3 = boto3.resource('s3')
        ing_bucket = f'{self.fm_prefix}-time-drvn-inbound-{self.ingestion_region}'
        bucket = s3.Bucket(ing_bucket)
        try:
            file_str = str()
            for obj in bucket.objects.filter(Prefix=f'init/{self.src_sys_id}/{self.asset_id}/'):
                body = obj.get()['Body'].read()
                output = str(body, 'UTF-8')
                file_str = file_str + output
            data_str = "[{}]".format(file_str.replace("}{", "},{"))
            data_bytes = bytes(data_str, 'utf-8')
            output_obj = s3.Object(f'{self.fm_prefix}-{self.src_sys_id}-{self.ingestion_region}',
                                   f'{self.asset_id}/init/{self.timestamp}/streaming_file.json')
            output_obj.put(Body=data_bytes)
            for obj in bucket.objects.filter(Prefix=f'init/{self.src_sys_id}/{self.asset_id}/'):
                s3.Object(obj.bucket_name, obj.key).delete()
        except Exception as e:
            logger.write(message=str(e))

    def move_streaming_file_to_processed(self):
        s3 = boto3.resource('s3')
        ing_source_bucket = f'{self.fm_prefix}-{self.src_sys_id}-{self.ingestion_region}'
        ing_bucket = f'{self.fm_prefix}-time-drvn-inbound-{self.ingestion_region}'
        src_bucket_obj = s3.Bucket(ing_source_bucket)
        dest_bucket_obj = s3.Bucket(ing_bucket)
        try:
            for obj in src_bucket_obj.objects.filter(Prefix=f"{self.asset_id}/init/{self.timestamp}/"):
                copy_source = {
                    'Bucket': ing_source_bucket,
                    'Key': obj.key
                }
                file_name = obj.key.split("/")[3]
                dest_bucket_obj.copy(copy_source, f"processed/{self.src_sys_id}/{self.asset_id}/{file_name}")
        except Exception as e:
            logger.write(message=str(e))
