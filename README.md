# Python to ELK data Pipeline

This project implements a pipeline to extract data from an Oracle database using SQL, transform the data as per business requirements, and index it into an Elasticsearch (ELK) instance. The pipeline ensures efficient data handling, transformation, and indexing for analytics and reporting purposes.

## Features

**SQL Execution:** Reads and executes SQL queries stored in external .txt files.

**Database Interaction:** Uses cx_Oracle to connect to an Oracle database and fetch data.

**Data Transformation:** Processes and transforms the fetched data into a desired format.

**Elasticsearch Integration:** Indexes transformed data into an Elasticsearch instance using elasticsearch-py and helpers.bulk.

**Error Handling:** Includes robust error handling mechanisms for database and Elasticsearch operations.

**Timezone Awareness:** Ensures consistent timestamps using timezone handling via pytz.
________________________________________

## Workflow
1.	Input Query: SQL query is read from an external Query.txt file.
2.	Database Connection: Connects to the Oracle database using credentials stored in the script.
3.	Data Fetching: Executes the SQL query and fetches data into a Pandas DataFrame.
4.	Transformation: Converts and transforms data into a JSON format compatible with Elasticsearch.
5.	Elasticsearch Connection: Establishes a connection to the Elasticsearch instance.
6.	Index Management: Checks if the target index exists in Elasticsearch; creates it if not.
7.	Data Indexing: Inserts the transformed data into the specified Elasticsearch index.
8.	Logging and Monitoring: Tracks task progress and logs errors for troubleshooting.
________________________________________

## Repository Structure

├── customer360_parameters.py  # Configuration parameters for database and Elasticsearch

├── main.py                    # Main script to execute the pipeline

├── Query.txt                  # SQL query file

├── requirements.txt           # Python dependencies

└── README.md                  # Project description
________________________________________

## Prerequisites
•	Python 3.8 or higher

•	Oracle Database with access credentials

•	Elasticsearch instance (v7.10+ recommended)

Python Dependencies
Install dependencies using: pip install -r requirements.txt
Required Python Packages

•	cx_Oracle
•	pandas
•	elasticsearch
•	elasticsearch-helpers
•	pytz
________________________________________

## Configuration

### Database Credentials
Update the db_credential dictionary in main.py:

db_credential = {
    "host_address": "your_database_host",
    "port": "your_database_port",
    "service_name": "your_service_name",
    "user_name": "your_user_name",
    "password": "your_password"
}

### Elasticsearch Credentials
Update the es_credential dictionary in main.py:

es_credential = {
    "host_address": "http://your_elasticsearch_host:9200",
    "user_name": "your_elasticsearch_username",
    "password": "your_elasticsearch_password"
}
________________________________________

## How to Run
1.	Import all .csv file in your database for tables.
2.	Test the sql query in database.
3.	Place your Python script in VScode and test the SQL database and ELK connections.
4.	Execute the Python script with the target Elasticsearch index name as an argument:

________________________________________
## Key Functions
1.	**database_connection():**  Establishes a connection to the Oracle database.
2.	**get_db_data(query, conn):** Fetches data by executing the given SQL query.
3.	**data_conversion(new_data_frame):**  Converts data into JSON format.
4.	**transform_new_data(index_name, result):** Transforms data for Elasticsearch compatibility.
5.	**create_es_connection(es_credential):**  Establishes a connection to Elasticsearch.
6.	**index_creation_in_elastic_search(es, index_name):**  Creates an index in Elasticsearch.
7.	**data_insertion_in_elastic_search(es, index_name, data):**  Inserts data into Elasticsearch.
________________________________________
### Logging and Monitoring
The script logs the progress and timestamps of each stage of the pipeline, allowing for better monitoring and debugging.
________________________________________
## Error Handling
•	Database Errors: Handles connection or query-related issues with detailed traceback logs.
•	Elasticsearch Errors: Manages indexing or connection errors, ensuring proper recovery.
________________________________________
## Future Enhancements 
•	Add support for multiple SQL queries in a single execution.
•	Automate index lifecycle management in Elasticsearch.
•	Include support for incremental data loading based on timestamps.
•	Implement a configuration file for better parameter management.
________________________________________
## License
This project is licensed under the MIT License. See the LICENSE file for details.

