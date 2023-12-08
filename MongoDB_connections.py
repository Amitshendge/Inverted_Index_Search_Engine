from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class Mongo():
    def __init__(self, config):
        self.config = config
        self.client = MongoClient(config['connection_string'], server_api=ServerApi('1'),connect=False)
        self.db = self.client[config['db_name']]
        self.data_collection = self.db[config['data_collection_name']]
        self.index_collection = self.db[config['index_collection_name']]

    def mongo_bulk_write(self, doc_list):
        """
        MongoDB Bulk Wite function used to pass the Update query for inverted index
        """
        try:
            self.index_collection.bulk_write(doc_list)
        except Exception as e:
            print('Errro Occured during bulk write to index collection')

    def drop_db(self, db_name):
        """
        MongoDB Delete DataBase
        """
        try:
            self.client.drop_database(db_name)
            print(f"\"{db_name}\" database has been deleted successfully.")
        except Exception as e:
            print('Errro Occured during Deleting Database')
