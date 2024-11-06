def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def upload_to_mongo(ti, **context):
    try:
        hook = MongoHook(mongo_conn_id='mongoid')
        client = hook.get_conn()
        db = client.MyDB
        currency_collection = db.currency_collection
        print(f"Connected to MongoDB - {client.server_info()}")
        
        d = json.loads(ti.xcom_pull(task_ids='get_data'))
        currency_collection.insert_one(d)
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")