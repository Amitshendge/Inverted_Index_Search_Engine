import utils
import os

"""
This is a testing class does not deal with any functionality in code.
"""

class maaain(utils.utilitis):
    def __init__(self):
        print(type(self.read_json_file('doc_count.json')))
# utils.testing()

obj = maaain()
print(obj)
print(os.path.join(os.path.dirname(__file__),'config.json'))