import pymongo

def get_connection():
    db_name=None
    try:
        connection_url =pymongo.MongoClient('mongodb://localhost:27017/')
        db_name=connection_url["automation_config"]
        print(db_name)
    except Exception as exception:
        print(exception)
    return db_name

def insert_single_doc(collection_name,query,projection=None):
    collection_data=None
    try:
        db_name=get_connection()
        collection_name=db_name[collection_name]
        print(collection_name)
        collection_data=collection_name.insert_one(query,projection)
        print(collection_data)
    except Exception as exception:
        print(exception)
    return collection_data

# if __name__ == "__main__":      
#     insert_single_doc("config",{"name":"Faisal","age":22})

def get_single_doc(collection_name,query,projection=None):
    collection_data=None
    try:
        db_name=get_connection()
        collection_name=db_name[collection_name]
        print(collection_name)
        collection_data=collection_name.find_one(query,projection)
        print(collection_data)
    except Exception as exception:
        print(exception)
    return collection_data

if __name__ == "__main__":
    get_single_doc("config",{"name":"email_config"},{"_id":0,"name":0})         
#     get_single_doc("config",{"name":"Faisal"},{"_id":0})    

def update_single_doc(collection_name,query,projection=None):
    collection_data=None
    try:
        db_name=get_connection()
        collection_name=db_name[collection_name]
        print(collection_name)
        collection_data=collection_name.update_one(query,projection)
        print(collection_data)
    except Exception as exception:
        print(exception)
    return collection_data

# if __name__ == "__main__":      
    # update_single_doc("config",{"username":"sidiqfaisal30@gmail.com"},{"$set":{"name":"email_config"}})   
    

def insert_multiple_doc(collection_name,query,projection=None):
    collection_data=None
    try:
        db_name=get_connection()
        collection_name=db_name[collection_name]
        print(collection_name)
        collection_data=collection_name.insert_many(query,projection)
        print(collection_data)
    except Exception as exception:
        print(exception)
    return collection_data

def find_single_doc(collection_name,query,projection=None):
    collection_data=None
    try:
        db_name=get_connection()
        collection_name=db_name[collection_name]
        print(collection_name)
        collection_data=collection_name.find_one(query,projection)
        print(collection_data)
    except Exception as exception:
        print(exception)
    return collection_data

    


