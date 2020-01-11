import sys

sys.path.append('../')
import mrop


def row_counter(state, row):
    state['row_count'] += 1
    return state


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

graph = mrop.ComputeGraph() \
    .fold(folder=row_counter, start_state={'row_count': 0})
graph.source = data
graph.compile()
graph.compute()


def test_fold():
    assert graph.result[0]['row_count'] == len(data)
