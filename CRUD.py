from typing import Dict, Any, List
from pymongo import MongoClient

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self , USER = 'aacuser', PASS = 'SNHU1234', HOST = 'nv-desktop-services.apporto.com',
                 PORT = 30842, DB = 'AAC', COL = 'animals'):
        # Initializing the MongoClient. This helps to
        # access the MongoDB databases and collections.

        # Cade-Bray: I've updated this init so the default values are parameters instead of hard-coded.
        #            This will allow the user to specify their own values when creating the object if needed.

        # Initialize Connection
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client[f'{DB}']
        self.collection = self.database[f'{COL}']
        self.client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to your MongoDB!")

    # Get the next record number. We're handling record number generation. We assume that the record number is an int
    # and incremented by one each time.
    def get_next_rec_num(self):
        query = [
            {
                "$group": {
                    "_id": None,
                    "maxNumber": {"$max": "$rec_num"}
                }
            }
        ]

        result = list(self.database.animals.aggregate(query))

        if result and "maxNumber" in result[0]:
            return result[0]["maxNumber"] + 1
        else:
            return 1

    # Complete this create method to implement the C in CRUD.
    def create(self, data : List[Dict[str, Any]]):
        if not data:
            raise ValueError("No data to insert")
            
        ids = []
        for doc in data:
            # remove the rec_num if it exists
            if "rec_num" in doc:
                del doc["rec_num"]
            
            # remove the _id if it exists
            if "_id" in doc:
                del doc["_id"]
            
            # add the rec_num
            doc["rec_num"] = self.get_next_rec_num()
            result = self.database.animals.insert_one(doc)
            ids.append(result.inserted_id)
            
        return True, ids

    # Read method to implement the R in CRUD.
    def read(self, data : Dict[str, Any]):
        if data is not None:
            # Find the data in the collection
            result = self.database.animals.find(data, {"_id": False})
            # Since it returns a cursor, we need to convert it to a list
            return list(result)
        else:
            return []

    # Update method to implement the U in CRUD.
    def update(self, data : Dict[str, Any], new_data : Dict[str, Any]):
        if not data:
            raise ValueError("No data to update")
        elif not new_data:
            raise ValueError("No new data to update with")
        else:
            affected = self.database.animals.update_many(data, {"$set": new_data})
            return affected.modified_count

    # Delete method to implement the D in CRUD.
    def delete(self, data : Dict[str, Any]):
        if not data:
            raise ValueError("No data to delete")
        else:
            deleted = self.database.animals.delete_many(data)
            return deleted.deleted_count