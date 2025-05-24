import os
import logging
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from typing import List, Dict, Optional, Any


LOG_LEVEL = os.getenv("LOG_LEVEL")
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
COUNTER_BUCKET_NAME = os.getenv("COUNTER_BUCKET_NAME")
COUNTER_FILE_NAME = os.getenv("COUNTER_FILE_NAME")
QUERIES_FOLDER = os.getenv("QUERIES_FOLDER")

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


bq_client: Optional[bigquery.Client] = None
gcs_client: Optional[storage.Client] = None 
counter_bucket: Optional[storage.Bucket] = None 


def initialize_bigquery_client():
    """
    Initializes the BigQuery client for accessing BigQuery datasets.
    
    This function sets up the BigQuery client using the project ID from environment variables. 
    If the client has already been initialized, it does nothing. If the initialization fails, 
    an exception is raised.
    
    Raises:
        ValueError: If the PROJECT_ID environment variable is not set.
        Exception: If the BigQuery client cannot be initialized.
    """
    global bq_client, PROJECT_ID
    if bq_client:
        return

    if not PROJECT_ID:
        logging.critical("GCP PROJECT_ID environment variable not set.")
        raise ValueError("GCP PROJECT_ID environment variable not set.")
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        logging.info(f"BigQuery client initialized for project: {PROJECT_ID}")
    except Exception as e:
        logging.critical(f"Failed to initialize BigQuery client: {e}")
        raise


def initialize_gcs_client():
    """
    Initializes the GCS client and retrieves the counter bucket.

    This function sets up the Google Cloud Storage (GCS) client and accesses the specified
    bucket used for storing the execution counter. If the client or bucket cannot be accessed,
    an exception is raised.

    Raises:
        ValueError: If the COUNTER_BUCKET_NAME environment variable is not set.
        Exception: If the GCS client cannot be initialized or the specified bucket cannot be accessed.
    """
    global gcs_client, counter_bucket, COUNTER_BUCKET_NAME

    if counter_bucket:
        return

    if not COUNTER_BUCKET_NAME:
        logging.critical("COUNTER_BUCKET_NAME environment variable not set. Cannot access counter file.")
        raise ValueError("COUNTER_BUCKET_NAME environment variable not set.")

    try:
        gcs_client = storage.Client()
        counter_bucket = gcs_client.get_bucket(COUNTER_BUCKET_NAME)
        logging.info(f"GCS client initialized and bucket '{COUNTER_BUCKET_NAME}' accessed for counter.")
    except Exception as e:
        logging.critical(f"Failed to initialize GCS client or access bucket '{COUNTER_BUCKET_NAME}': {e}")
        raise


def read_execution_counter() -> int:
    """
    Reads the execution counter value from a GCS object.

    This function attempts to read the counter value stored in a GCS object. If the object
    does not exist or the data is invalid, it returns a default counter value of 0.

    Returns:
        int: The execution counter value.

    Raises:
        ValueError: If the COUNTER_FILE_NAME environment variable is not set.
        Exception: If there is any unexpected error while reading the counter.
    """
    initialize_gcs_client()
    if not COUNTER_FILE_NAME:
        logging.critical("COUNTER_FILE_NAME environment variable not set.")
        raise ValueError("COUNTER_FILE_NAME environment variable not set.")

    blob = counter_bucket.blob(COUNTER_FILE_NAME)

    try:
        counter_str = blob.download_as_text()
        counter_value = int(counter_str.strip())
        logging.info(f"Successfully read counter from GCS object '{COUNTER_FILE_NAME}' in bucket '{COUNTER_BUCKET_NAME}': {counter_value}")
        return counter_value
    except NotFound:
        logging.info(f"Counter object '{COUNTER_FILE_NAME}' not found in bucket '{COUNTER_BUCKET_NAME}'. Assuming first execution (counter 0).")
        return 0
    except ValueError:
        logging.error(f"Counter object '{COUNTER_FILE_NAME}' contains invalid integer data. Treating as 0.")
        return 0
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading counter object '{COUNTER_FILE_NAME}': {e}")
        return 0


def write_execution_counter(counter_value: int):
    """
    Writes the execution counter value to a GCS object.

    This function uploads the given counter value as a string to a GCS object for persistent 
    storage. It logs success or failure during the upload process.
    
    Args:
        counter_value (int): The counter value to be written to the GCS object.

    Raises:
        ValueError: If the COUNTER_FILE_NAME environment variable is not set.
        Exception: If there is any error while writing the counter value to GCS.
    """
    initialize_gcs_client()
    if not COUNTER_FILE_NAME:
        logging.critical("COUNTER_FILE_NAME environment variable not set.")
        raise ValueError("COUNTER_FILE_NAME environment variable not set.")

    blob = counter_bucket.blob(COUNTER_FILE_NAME)

    try:
        blob.upload_from_string(str(counter_value))
        logging.info(f"Successfully wrote counter '{counter_value}' to GCS object '{COUNTER_FILE_NAME}'.")
    except Exception as e:
        logging.error(f"Failed to write counter '{counter_value}' to GCS object '{COUNTER_FILE_NAME}': {e}")


def load_queries_from_folder(folder_path: str) -> list[str]:
    """
    Loads all SQL queries from files within a specified folder.

    This function recursively searches the specified folder for SQL files, reads their contents,
    and returns a list of queries.

    Args:
        folder_path (str): The relative or absolute path to the folder containing SQL query files.

    Returns:
        List[str]: A list of SQL query strings read from the files.

    Logs a warning if the folder does not exist or if no SQL queries are found.
    """
    source_dir = os.path.dirname(os.path.abspath(__file__))
    full_folder_path = os.path.join(source_dir, folder_path)

    all_queries = []
    if not os.path.exists(full_folder_path):
        logging.warning(f"Queries folder '{full_folder_path}' does not exist.")
        return all_queries

    for root, _, files in os.walk(full_folder_path):
        for filename in files:
            if filename.endswith(".sql"):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, "r") as f:
                        query = f.read().strip()
                        if query:
                            all_queries.append(query)
                except Exception as e:
                    logging.error(f"Error reading query file {filepath}: {e}")

    logging.info(f"Loaded {len(all_queries)} queries from '{full_folder_path}'.")
    return all_queries


def execute_query(sql: str) -> Optional[List[Dict]]:
    """
    Executes a SQL query in BigQuery.

    This function runs the provided SQL query using the initialized BigQuery client and 
    returns the results as a list of dictionaries, where each dictionary represents a row.

    Args:
        sql (str): The SQL query string to be executed.

    Returns:
        Optional[List[Dict]]: The results of the query as a list of dictionaries, or None if 
        the query execution failed.

    Logs the success or failure of the query execution.
    """
    if not bq_client:
        logging.error("BigQuery client not initialized.")
        return None

    logging.info(f"Executing query: {sql}")
    try:
        query_job = bq_client.query(sql)
        results = [dict(row.items()) for row in query_job]
        logging.info(f"Query returned {len(results)} rows.")
        return results
    except Exception as e:
        logging.error(f"Error executing query: {e}", exc_info=True)
        return None



def main(request) -> tuple[dict, int]:
    """
    Cloud Function HTTP Trigger entry point.

    This function serves as the entry point for an HTTP-triggered Cloud Function. It reads the 
    execution counter from a GCS object, executes a set of BigQuery queries if the counter is 0, 
    and increments the counter in GCS after execution. If the counter is not 0, the queries are skipped.

    Args:
        request (Any): The HTTP request object. Typically, this will be a Flask request object.

    Returns:
        tuple[dict, int]: A tuple containing a dictionary with status and message, and the HTTP status code.
    
    Raises:
        ValueError: If there is a configuration error (e.g., missing environment variables).
        Exception: If there is an unexpected error during the execution of the function.
    """
    try:
        current_counter = read_execution_counter()
        logging.info(f"Current execution counter read from GCS: {current_counter}")

        if current_counter == 0:
            logging.info("GCS counter is 0. Proceeding with query execution.")
            initialize_bigquery_client()

            all_queries = load_queries_from_folder(QUERIES_FOLDER)

            if not all_queries:
                logging.warning(f"No SQL queries found in '{QUERIES_FOLDER}'. Query execution skipped.")
                return {"status": "warning", "message": f"No SQL queries found in '{QUERIES_FOLDER}'. Query execution skipped."}, 200

            execution_results = []
            execution_succeeded = False

            try:
                logging.info(f"Executing all {len(all_queries)} queries.")
                for query in all_queries:
                    result = execute_query(query)
                    execution_results.append({
                        "query": query,
                        "result_rows": len(result) if result is not None else 0,
                        "status": "success" if result is not None else "error"
                    })
                    if result is None:
                         execution_succeeded = False
                    else:
                         execution_succeeded = True 
                execution_succeeded = True

            except Exception as e:
                 logging.exception("An unexpected error occurred during the query execution loop.")
                 execution_succeeded = False

            next_counter = current_counter + 1
            write_execution_counter(next_counter)

            if execution_succeeded:
                 logging.info("Successfully completed query execution and incremented GCS counter.")
                 return {"status": "success", "message": f"Executed {len(all_queries)} queries from '{QUERIES_FOLDER}' and incremented counter.", "results": execution_results}, 200
            else:
                 logging.error("Query execution phase did not complete successfully. GCS counter was still incremented.")
                 return {"status": "error", "message": "An error occurred during query execution. Counter was incremented.", "results": execution_results}, 500


        else:
            logging.info("queries have been already executed.")
            return {"status": "success", "message": f"Query execution skipped. Counter is already {current_counter} (must be 0 to run queries)."}, 200

    except ValueError as ve:
         logging.error(f"Configuration error: {ve}")
         return {"error": f"Configuration error: {ve}"}, 500

    except Exception as e:
        logging.exception("An unexpected error occurred in the main function flow.")
        return {"error": "Internal Server Error. Check function logs for details."}, 500