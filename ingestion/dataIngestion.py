import sys
import json
from awsglue.utils import getResolvedOptions
from utils.dataIngestionUtils import *
from connector.pg_connect import Connector


def get_global_config():
    """
    Utility method to get global config file
    :return:JSON
    """
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
try:
    config_dict = get_global_config()
    conn = Connector(config_dict["db_secret"], config_dict["db_region"],schema=config_dict["schema"])
    ing_db = IngestionAttr(conn, config_dict, args)
    if ing_db.ing_pattern == "database":
        data = ing_db.pull_data_from_db()
        ing_db.drop_data_to_s3(data)
    if ing_db.ing_pattern == "file":
        ing_db.copy_file_between_buckets()
        ing_db.move_file_within_bucket()
    if ing_db.ing_pattern == "stream":
        ing_db.merge_and_copy_streaming_file_to_raw()
        ing_db.move_streaming_file_to_processed()
    if ing_db.derive_schema_ind:
        ing_db.delete_asset_if_present()
        ing_db.derive_schema()
    ing_db.insert_record_in_catalog_tbl()
    conn.close()
except Exception as e:
    logger.write(message=str(e))
    raise sys.exit()
