import json
import random
import os

class random_search():
    def generate_random_search(self, file_paths):
        """
        Randomly select 3 movie names from each file in dataset and store into movies.json file.
        Those movie names will be used to search when that file will be uploaded and track seach perfomance.
        """
        movie_list = {}
        for file_path in file_paths:
            f = open(os.path.join(os.path.dirname(__file__),file_path), 'r')
            list_obj = json.load(f)
            random_obj = random.choices(list_obj,k=3)
            movie_list[file_path] = [i['movie'] for i in random_obj]

        with open(os.path.join(os.path.dirname(__file__),"movies.json"), "w") as g:
            json.dump(movie_list, g, indent=4)
