from db_update import * # load_data_to_db, delete_intersections, get_intersections, process_data, read_excel_files, load_excel_sheets, transform_and_load_dict, create_db_engine, create_ssh_tunnel
from params import * # DATA, DICT_PATH, LIST_OF_SHEETS
from logging_config import * # logger


# Main function
@log_function_execution
def main():
        
    # Create SSH tunnel
    with create_ssh_tunnel() as ssh_tunnel:
        
        # Create database engine
        engine = create_db_engine(ssh_tunnel)
        
        # Create session
        Session = sessionmaker(bind=engine)
    
        with Session() as session:
            # Iterate over dictionary items
            for table_name, table_info in DATA.items():  
                logging.info(f"Processing table: {table_name}")
            
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
                load_data_to_db(df, engine, table_name, table_info["IF_EXISTS"])   
                
                # Create Dict 
                dfs = load_excel_sheets(DICT_PATH, LIST_OF_SHEETS)
                
                #transform and load dict data to database
                transform_and_load_dict(engine, dfs)

if __name__ == '__main__':
    main()