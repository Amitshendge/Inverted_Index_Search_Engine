import os
import ElasticSearch_upload_docs
import ElasticSearch_fetch_docs
import MongoDB_connections
import random_search_movies
import time
import utils

config_file_path = os.path.join(os.path.dirname(__file__),'config.json')
util = utils.utilitis()
def upload_file(mong_conn, file_path, drop_database=True):
    """
    Drop the DataBase if drop_database is True.
    Create a object of Upload document class and call upload_docs function with file path.
    """
    if drop_database:
        mong_conn.drop_db(mong_conn.config['db_name'])
    print("Started Uploading Documents into MongDB")
    ES_upload = ElasticSearch_upload_docs.ES_upload(mong_conn)
    ES_upload.upload_docs(file_path)

def search_documents(mong_conn, search_text):
    """
    Create a object of Fetch document class and call search_docs function with text to be searched in DataBase.
    """
    print("Started Searching Documents into MongDB")
    fetch = ElasticSearch_fetch_docs.ES_fetch(mong_conn)
    fetch.search_docs(search_text)

def save_count(mong_conn):
    """
    Saves a Count of documents present in DataBase into doc_count.json overwrites the count everytime.
    Used to track perfomance with respect to number of documents present in DataBase.
    """
    print("Started counting number of documents in DB for perfomance analysis")
    t_start = time.time()
    doc_count = mong_conn.data_collection.count_documents({})
    count_file = open(os.path.join(os.path.dirname(__file__),'doc_count.json'), 'w')
    count_file.write(str(doc_count))
    t_end = time.time()
    print(f"Time taken to count documents from DB {t_end-t_start} sec")

if __name__ == "__main__":
    t_stat = time.time()
    config = util.read_json_file(config_file_path)
    mong_conn = MongoDB_connections.Mongo(config)
    file_paths = [
    'dataset/part-01.json',
    'dataset/part-02.json',
    'dataset/part-03.json',
    'dataset/part-04.json',
    'dataset/part-05.json',
    'dataset/part-06.json'
    ]

    # movie_search_lst = []
    # fetchfile = open(os.path.join(os.path.dirname(__file__),'fetch_stat.csv'), 'w')
    # util.write_line(fetchfile, ['No of Documents', 'Movie Name', 'Inverted Index Search Time', 'Normal Index Search Time'])
    # fetchfile.close()
    # uploadfile = open(os.path.join(os.path.dirname(__file__),'upload_stat.csv'), 'w')
    # util.write_line(uploadfile, ['File Path', 'No of Documents', 'Time to upload into Data Coll', 'Total Time to upload and create invetd Index'])
    # uploadfile.close()

    # if not os.path.isfile(os.path.join(os.path.dirname(__file__),'movies.json')):
    #     print("Stared storing Random movie names from data files for searching in movies.json file")
    #     random_generator = random_search_movies.random_search()
    #     random_generator.generate_random_search(file_paths)
    # for idx, file_path in enumerate(file_paths):
    #     random_movies = util.read_json_file(os.path.join(os.path.dirname(__file__),'movies.json'))
    #     upload_file(mong_conn, os.path.join(os.path.dirname(__file__),file_path), idx==0)
    #     save_count(mong_conn)
    #     movie_search_lst = []
    #     for movie_name in random_movies[file_path]:
    #         movie_search_lst.append(movie_name)
    #     for movie_search_name in movie_search_lst:
    #         search_documents(mong_conn, movie_search_name)
    # mong_conn.client.close()
    # t_end = time.time()
    # print(f"Total time taked to run this program: {t_end-t_stat} sec")

movie_search_name = 'The Rifleman: Dead Cold Cash (1960) Season 3, Episode 9'

search_documents(mong_conn, movie_search_name)