import json
import os

class utilitis():
    def __init__(self) -> None:
        pass

    def read_json_file(self,file_path):
        """
        Read the JSON file by taking file path as argument and returns the Dictionary object of JSON file.
        """
        f = open(os.path.join(os.path.dirname(__file__),file_path), 'r')
        list_obj = json.load(f)
        return list_obj

    def write_line(self, file_obj, content_list):
        """
        Write a line from words in a list using file Open object.
        We will be writing into csv file so joining words in list with ','.
        """
        file_obj.write(','.join([str(i) for i in content_list]))
        file_obj.write('\n')
