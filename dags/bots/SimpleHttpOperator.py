# bots/custom_mongo_operator.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json

class CustomMongoOperator(BaseOperator):
    @apply_defaults
    def __init__(self, mongo_conn_id, database, collection, data=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.database = database
        self.collection = collection
        self.data = data

    def execute(self, context):
        # Fetch data from XCom if `data` is not provided directly
        data_to_insert = self.data or context['ti'].xcom_pull(task_ids='get_data')
        if not data_to_insert:
            self.log.error("No data provided for MongoDB insertion.")
            return

        # Connect to MongoDB
        try:
            client = MongoClient(self.mongo_conn_id)
            db = client[self.database]
            collection = db[self.collection]
            # Insert the data
            if isinstance(data_to_insert, dict):
                collection.insert_one(data_to_insert)
                self.log.info("Data inserted as a single document.")
            elif isinstance(data_to_insert, list):
                collection.insert_many(data_to_insert)
                self.log.info(f"Inserted {len(data_to_insert)} documents.")
            else:
                self.log.error("Data format not supported for MongoDB insertion.")
        except Exception as e:
            self.log.error(f"Error connecting to MongoDB or inserting data: {e}")
        finally:
            client.close()
