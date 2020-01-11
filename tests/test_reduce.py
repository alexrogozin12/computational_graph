import re
import sys
from collections import defaultdict

sys.path.append('../')
import mrop


def reducer_count_words(rows):
    yield {
        'doc_id': rows[0]['doc_id'],
        'word_count': len(rows)
    }


data = [
    {'doc_id': 'onegin', 'word': 'Tatyana'},
    {'doc_id': 'onegin', 'word': 'verila'},
    {'doc_id': 'onegin', 'word': 'predyanam'},
    {'doc_id': 'onegin', 'word': 'prostonarognoy'},
    {'doc_id': 'onegin', 'word': 'stariny'},
    {'doc_id': 'algebra', 'word': 'prostranstvo'},
    {'doc_id': 'algebra', 'word': 'mnogochlenov'},
    {'doc_id': 'algebra', 'word': 'Teita'},
    {'doc_id': 'algebra', 'word': 'gomeomorphno'}
]

reduce = mrop.Reduce(reducer=reducer_count_words, key='doc_id')
reduce.set_input_gen(data)
res = list(reduce)

true_res = [
    {'doc_id': 'onegin', 'word_count': 5},
    {'doc_id': 'algebra', 'word_count': 4}
]


def test_reduce():
    assert res == true_res
