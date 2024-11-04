import pymongo

connection_url =pymongo.MongoClient('mongodb://localhost:27017/')
db_name=connection_url["automation_config"]
print(db_name)
collection_name=db_name["config"]
print(collection_name)
collection_data=collection_name.insert_one({"username":"sidiqfaisal30@gmail.com","password":"juva yapw wakj apxt"})

print(collection_data)