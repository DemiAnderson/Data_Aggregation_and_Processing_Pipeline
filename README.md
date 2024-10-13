# DATABASE CONNECTION AND UPDATE SCRIPT

The script processes data from three folders containing data from different sources.
Overview:

This script is designed to automate the process of data accumulation, transformation, and loading into a database. It checks for new data files in predefined directories, processes the data, and uploads it into the database, ensuring that existing data is updated as needed.

# Prerequisites:

    Python 3.x
    A requirements.txt file is provided for installing the necessary dependencies:

    bash

    pip install -r requirements.txt

    Note: This code is intended for use in a closed company environment, and therefore, the db_config file is not included in this repository. You will need to set up your own database configuration separately.

# Installation and Setup:

## Install required dependencies:

bash

    pip install -r requirements.txt

    Set up your database configuration in a separate db_config file. Make sure the necessary credentials and connection settings are correctly specified.

# Usage:

## Run the script with:

bash

    python main.py

If there are specific arguments (like folder paths or dates), document them here.

# Error Handling:

    Logging: A separate file, logging_config, contains a logging function that is used as a decorator. This logs detailed information about the execution of each function, including file names and processing steps. Every operation, such as reading or writing files, is logged for easier debugging and auditing.

    Error catching: The db_update file contains a function for catching and handling errors, which is also used as a decorator. It ensures that any exceptions raised during the execution are caught and logged, preventing the script from failing silently. This helps maintain a smooth workflow and allows issues to be quickly identified and resolved.

This project is licensed under the MIT License.
