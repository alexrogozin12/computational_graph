import re
import sys

sys.path.append('../')
import mrop


def split_input(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


data = [{'doc_id': 'my_text', 'text': 'Tatyana verila predanyam!! '
                                      'prostonarodnoy stariny...'}]

map = mrop.Map(split_input)
map.set_input_gen(data)
res = list(map)

true_res = [{'doc_id': 'my_text', 'word': 'tatyana'},
            {'doc_id': 'my_text', 'word': 'verila'},
            {'doc_id': 'my_text', 'word': 'predanyam'},
            {'doc_id': 'my_text', 'word': 'prostonarodnoy'},
            {'doc_id': 'my_text', 'word': 'stariny'}]


def test_map():
    assert res == true_res
