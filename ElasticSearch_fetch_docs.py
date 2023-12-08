import pymongo
import time
import utils
import os

class ES_fetch(utils.utilitis):
    def __init__(self, mong_conn):
        self.log_stats = []
        self.mong_conn = mong_conn
        self.search_field = self.mong_conn.config['search_field']
        self.csvfile = open(os.path.join(os.path.dirname(__file__),'fetch_stat.csv'), 'a',encoding="utf-8")
        if not self.mong_conn.data_collection.index_information().get(f'{self.search_field}_text'):
            self.mong_conn.data_collection.create_index([(self.search_field, pymongo.TEXT)])

    def search_document(self, doc_ids, collection):
        """
        MongoDB find one document quey with unique ObjectId from list of Object Ids
        """
        return [collection.find_one({'_id': doc_id}) for doc_id in doc_ids]

    def separate_search(self, input_text):
        """
        Splitting text into words and send each word to search document in inverted index to obtain actual 
        Object ids of stored documents. Then taking the intersection of all the lists of ObjectIds and obtain actual data stored.
        """
        keywords = input_text.split()
        final_doc_ids = [doc['ids'] for doc in self.search_document(keywords,self.mong_conn.index_collection) if doc]
        if not final_doc_ids:
            return []
        common_elements = set(final_doc_ids[0])
        for sublist in final_doc_ids[1:]:
            common_elements = common_elements.intersection(sublist)
        return self.search_document(common_elements,self.mong_conn.data_collection)
        
    def search_docs(self, search_text):
        """
        Function to log the perfomance numbers and call the fucntions which search using inveted index method.
        Then search using MongoDB text search index and log the perfomance numbers
        """
        doc_count = self.read_json_file('doc_count.json')
        self.log_stats.append(doc_count)
        self.log_stats.append(search_text.replace(',',''))
        t_start = time.time()
        abc = [doc['movie'] for doc in self.separate_search(search_text)]
        print(abc)
        print(len(abc))
        t_end = time.time()

        self.log_stats.append(t_end-t_start)
        print(f"documents have been loaded using inverted index in {t_end-t_start} sec.")

        t_start = time.time()
        # Search documents using Text Index in MongoDB on certain feild
        defg = [doc['movie'] for doc in self.mong_conn.data_collection.find({'$text':{'$search':f"\"{search_text}\""}})]
        print(defg)
        print(len(defg))
        t_end = time.time()
        self.log_stats.append(t_end-t_start)
        print(f"documents have been loaded using normal search index in {t_end-t_start} sec.")
        self.write_line(self.csvfile, self.log_stats)


