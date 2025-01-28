import logging
import snowflake.snowpark as snp
from snowflake.snowpark import Session, Window
from snowflake.snowpark.functions import sha1, col, lit, current_timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dv_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define table configurations
TABLE_CONFIG = {
    'HUB_CUSTOMER': {
        'source_stream': 'STG_CUSTOMER_STRM_OUTBOUND',
        'business_key': 'C_CUSTKEY',
        'hash_key': 'SHA1_HUB_CUSTOMER'
    },
    'HUB_ORDER': {
        'source_stream': 'DV_LAB.L00_STG.STG_ORDER_STRM_OUTBOUND',
        'business_key': 'O_ORDERKEY',
        'hash_key': 'SHA1_HUB_ORDER'
    }
    # ,
    # 'LNK_CUSTOMER_ORDER': {
    #     'source_stream': 'STG_ORDERS_STRM',
    #     'hash_key': 'SHA1_LNK_CUSTOMER_ORDER',
    #     'parent_hubs': ['HUB_CUSTOMER', 'HUB_ORDER']
    # },
    # 'SAT_CUSTOMER': {
    #     'source_stream': 'STG_CUSTOMER_STRM',
    #     'hash_key': 'SHA1_HUB_CUSTOMER',
    #     'attributes': ['C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 
    #                   'C_MKTSEGMENT', 'C_COMMENT', 'NATIONCODE']
    # },
    # 'SAT_ORDER': {
    #     'source_stream': 'STG_ORDERS_STRM',
    #     'hash_key': 'SHA1_HUB_ORDER',
    #     'attributes': ['O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE',
    #                   'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
    # }
    # ,
    # 'REF_NATION': {
    #     'source_stream': 'STG_NATION_STREAM',
    #     'business_key': 'NATIONCODE'
    # },
    # 'REF_REGION': {
    #     'source_stream': 'STG_REGION_STREAM',
    #     'business_key': 'REGIONCODE'
    # }
}

def create_session():
    connection_params = {
        "account": "AAPDKWQ-GT81863",
        "user": "basant029",
        "password": "Nailwab@271088",
        "role": "accountadmin",
        "warehouse": "compute_wh",
        "database": "dv_lab",
        "schema": "L00_stg"
    }
    return Session.builder.configs(connection_params).create()

def process_hub(session, table_name, source_df, config):
    logger.info(f"Processing Hub: {table_name}")
    business_key = config['business_key']
    hash_key = config['hash_key']
    
    # Add hash key and metadata columns to the source DataFrame
    source_df = source_df.withColumn(hash_key, sha1(col(business_key))) \
                         .withColumn("LDTS", current_timestamp()) \
                         .withColumn("RSCR", lit("SYSTEM"))
    
    # Get the target table
    target_table = session.table(table_name)
    
    # Define the merge condition
    merge_condition = f"target.{hash_key} = source.{hash_key}"
    
    # Define the actions for matched and not matched cases
    when_matched = None  # Hubs typically don't update existing records
    when_not_matched = {
        "INSERT": {
            hash_key: col(f"source.{hash_key}"),
            business_key: col(f"source.{business_key}"),
            "LDTS": col("source.LDTS"),
            "RSCR": col("source.RSCR")
        }
    }
    
    # Perform the merge operation
    target_table.merge(
        source_df,
        merge_condition,
        [when_matched] if when_matched else [],
        when_not_matched,
        target_alias="target",
        source_alias="source"
    )
    
    logger.info(f"Hub {table_name} processed successfully")

def process_link(session, table_name, source_df, config):
    logger.info(f"Processing Link: {table_name}")
    # Implement link processing logic
    pass

def process_satellite(session, table_name, source_df, config):
    logger.info(f"Processing Satellite: {table_name}")
    # Implement satellite processing logic
    pass

def process_reference(session, table_name, source_df, config):
    logger.info(f"Processing Reference: {table_name}")
    business_key = config['business_key']
    
    target_df = session.table(table_name)
    
    merge_condition = f"target.{business_key} = source.{business_key}"
    when_matched = {
        "UPDATE": {
            "LDTS": current_timestamp(),
            "RSCR": lit("SYSTEM"),
            # Add other columns as needed
        }
    }
    when_not_matched = {
        "INSERT": {
            business_key: col(f"source.{business_key}"),
            "LDTS": current_timestamp(),
            "RSCR": lit("SYSTEM"),
            # Add other columns as needed
        }
    }
    
    source_df.merge(target_df, merge_condition, [when_matched], when_not_matched, target.alias("target"), source_df.alias("source"))
    logger.info(f"Reference {table_name} processed successfully")

def process_delta(session, stream_name):
    logger.info(f"Checking stream: {stream_name}")
    stream_df = session.table(stream_name)
    
    if stream_df.count() == 0:
        logger.info(f"No changes in {stream_name}")
        return
    
    for table_name, config in TABLE_CONFIG.items():
        if config['source_stream'] == stream_name:
            source_df = stream_df.select(*[c for c in stream_df.columns if c != 'METADATA$ACTION'])
            
            if table_name.startswith('HUB'):
                process_hub(session, table_name, source_df, config)
            elif table_name.startswith('LNK'):
                process_link(session, table_name, source_df, config)
            elif table_name.startswith('SAT'):
                process_satellite(session, table_name, source_df, config)
            elif table_name.startswith('REF'):
                process_reference(session, table_name, source_df, config)

def main():
    try:
        session = create_session()
        logger.info("Session created successfully")
        
        # List of all streams to monitor
        streams = [config['source_stream'] for config in TABLE_CONFIG.values()]
        unique_streams = list(set(streams))
        
        for stream in unique_streams:
            process_delta(session, stream)
            
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise
    finally:
        session.close()
        logger.info("Session closed")

if __name__ == "__main__":
    main()