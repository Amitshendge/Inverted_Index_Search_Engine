import time
from pymongo import UpdateOne
import utils
import os

class ES_upload(utils.utilitis):
    def __init__(self, mong_conn):
        self.log_stats = []
        self.mong_conn = mong_conn
        self.search_field = self.mong_conn.config['search_field']
        self.csvfile = open(os.path.join(os.path.dirname(__file__),'upload_stat.csv'), 'a')

    def separator(self, json_obj, search_field):
        """
        Splitting text into words and return list of words
        """
        return json_obj[search_field].split()

    def insert_data_collection(self, list_obj):
        """
        Insert all the documents in Data Collection as it is and generating inverted index from recieved ObjectIds in response from MongDB.
        Returns a list of documents with word as key _id and id feild with ObjectID of actual document in data collection.
        """
        t_start = time.time()
        results = self.mong_conn.data_collection.insert_many(list_obj)
        t_end = time.time()
        self.log_stats.append(t_end-t_start)
        doc_ids = []
        for i, j in zip(list_obj, results.inserted_ids):
            obj_id = j
            for key in self.separator(i, self.search_field):
                doc_ids.append({"_id": key, "id": obj_id})
        return doc_ids

    def create_inverted_index(self, list_obj):
        """
        Here the returned list of docs from insert_data_collection is agreegated and combined into 
        one doc for one word with added count feild and list of all Object Ids found for same word.
        """
        doc_ids = self.insert_data_collection(list_obj)
        my_dic = {}
        for item in doc_ids:
            _id = item['_id']
            if _id not in my_dic:
                my_dic[_id] = {'_id': _id, 'ids': [], 'count': 0}
            my_dic[_id]['ids'].append(item['id'])
            my_dic[_id]['count'] += 1
        return list(my_dic.values())

    def update_inverted_index(self, list_obj):
        """
        Here the final list of documents is send to MongoDB update function with Upsert=True meanse update if exist or 
        create new document with count as increment to existing and ids append to existing array in MongoDB document for that word.
        """
        final_lst=[]
        grouped_list = self.create_inverted_index(list_obj)
        for doc in grouped_list:
            final_lst.append(UpdateOne({"_id": doc['_id']}, {"$inc": {"count": doc['count']}, "$push": {'ids':{'$each':doc['ids'],'$position':0}}}, upsert=True))
        self.mong_conn.mongo_bulk_write(final_lst)

    def upload_docs(self, upload_file_path):
        """
        Function to log the perfomance numbers and call the fucntions which uploads docs and creates inveted index.
        """
        t_start = time.time()
        self.log_stats.append(upload_file_path)
        list_obj = self.read_json_file(upload_file_path)
        self.log_stats.append(len(list_obj))
        self.update_inverted_index(list_obj)
        t_end = time.time()
        self.log_stats.append(t_end-t_start)
        print(f"The {len(list_obj)} documents have been loaded into Search Engine in {t_end-t_start} sec.")
        self.write_line(self.csvfile, self.log_stats)
