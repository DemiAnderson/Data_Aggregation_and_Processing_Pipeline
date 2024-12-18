
from db_update import (
    create_db_engine,
    create_ssh_tunnel,
    delete_intersections,
    distrib_files_to_target_dirs,
    get_intersections,
    load_excel_sheets,
    load_data_to_db,
    process_data,
    read_excel_files,
    refresh_materialized_views,
    transform_and_load_dict,
    sessionmaker
)
from fetch_data_process import fetch_external_data
from logging_config import logger, log_function_execution
from params import (
    DATA,
    DICT_PATH,
    LIST_OF_SHEETS,
    RAW_DATA_PATH,
    TARGET_KEYS,
    MAT_VIEWS
)

      
# Main function
@log_function_execution
def main():
    
    fetch_external_data(DATA["sales"]["FOLDER_PATH_IN"])
    # Distribute files from the raw data path to the target directories based on the specified keys  
    distrib_files_to_target_dirs(RAW_DATA_PATH, TARGET_KEYS)
        
    # Create SSH tunnel
    with create_ssh_tunnel() as ssh_tunnel:
        
        # Create database engine
        engine = create_db_engine(ssh_tunnel)
        
        # Create session
        Session = sessionmaker(bind=engine)
    
        with Session() as session:
            # Iterate over dictionary items
            for table_name, table_info in DATA.items():  
                logger.info(f"processing table: {table_name}")
            
                # Read Excel files
                df = read_excel_files(
                                    table_info["FOLDER_PATH_IN"], 
                                    table_info["FOLDER_PATH_OUT"], 
                                    table_info["SHEET"], 
                                    table_info["SKIP"], 
                                    table_info["COL_NAMES"]
                                    )

                # Process data
                df = process_data(df, table_info["COMPANIES"])
                                
                # Create intersections
                intersection_df = get_intersections(engine, df)
                
                # Remove intersections from the database
                delete_intersections(session, intersection_df, table_name)
                
                # Load data to database
                load_data_to_db(df, engine, session, table_name, table_info["IF_EXISTS"], table_info["FOLDER_PATH_IN"])   
                
            # Create Dicts 
            dicts = load_excel_sheets(DICT_PATH, LIST_OF_SHEETS)
            
            #transform and load dicts data to database
            transform_and_load_dict(engine, session, dicts)
            
            # Refreshing the materialized view
            for view in MAT_VIEWS:
                refresh_materialized_views(session, view)
            
            # End session
            session.commit()

if __name__ == '__main__':
    main()
