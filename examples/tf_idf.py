import math
import re
import mrop
from collections import defaultdict


def split_input(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


def row_counter(state, row):
    state['docs_count'] += 1
    return state


def reducer_unique(rows):
    yield rows[0]


def reducer_calc_idf(rows):
    word = rows[0]['word']
    docs_num = rows[0]['docs_count']
    docs_with_word_num = len(rows)
    yield {
        'word': word,
        'idf': math.log(docs_num / docs_with_word_num)
    }


def reducer_calc_tf(rows):
    num_words = len(rows)
    doc_id = rows[0]['doc_id']

    word_count = defaultdict(int)
    for row in rows:
        word_count[row['word']] += 1

    for word, count in word_count.items():
        yield {
            'doc_id': doc_id,
            'word': word,
            'tf': count / num_words
        }


def mapper_calc_tf_idf(row):
    yield {
        'doc_id': row['doc_id'],
        'word': row['word'],
        'tf_idf': row['tf'] * row['idf']
    }


def reducer_top_doc_counter(rows):
    rows.sort(key=lambda dct: dct['tf_idf'], reverse=True)
    yield {
        'term': rows[0]['word'],
        'index': [(row['doc_id'], row['tf_idf']) for row in rows[:3]]
    }


if __name__ == '__main__':
    source = '../data/text_corpus.txt'
    # source = '../data/input.txt'
    output = '../data/output.txt'

    split_word = mrop.ComputeGraph() \
        .map(split_input)

    count_docs = mrop.ComputeGraph() \
        .fold(row_counter, {'docs_count': 0})

    count_idf = mrop.ComputeGraph() \
        .input(split_word) \
        .sort(['doc_id', 'word']) \
        .reduce(reducer_unique, ['doc_id', 'word']) \
        .join(count_docs, strategy='cross') \
        .sort('word') \
        .reduce(reducer_calc_idf, 'word')

    calc_index = mrop.ComputeGraph() \
        .input(split_word) \
        .sort(['doc_id']) \
        .reduce(reducer_calc_tf, ['doc_id']) \
        .join(count_idf, strategy='left', key='word') \
        .map(mapper_calc_tf_idf) \
        .sort('word') \
        .reduce(reducer_top_doc_counter, key='word')

    calc_index.compile()

    with open(source, 'r') as in_file:
        subgraph_sources = {
            split_word: in_file,
            count_docs: in_file
        }
        calc_index.run(output=open(output, 'w'), subgraph_inputs=subgraph_sources, verbose=1)
