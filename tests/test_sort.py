import re
import sys
from collections import defaultdict

sys.path.append('../')
import mrop


def split_input(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


data = [
    {'doc_id': 'my_text',
     'text': 'Etot paren byl is teh, kto prosto lubit zhizn. '
             'Lubit prazdniki i grooomkiy smeh, shum dorog i vetra svist. '
             'On byl vesde, i vesde vlublyal v sebya tseliy svet. '
             'I gnal svoy baik, a ne limuzin. Takih druzey bolshe net!'
     }
]

graph = mrop.ComputeGraph() \
    .map(split_input) \
    .sort('word')
graph.source = data
graph.compile()
graph.compute()

res = [row['word'] for row in graph.result]
true_res = [word.lower() for word in re.findall(r'[\w]+', data[0]['text'])]
true_res = sorted(true_res)


def test_sort():
    assert res == true_res
