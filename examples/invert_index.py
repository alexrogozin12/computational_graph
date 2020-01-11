import math
import mrop
import re
from collections import defaultdict


def split_input(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


def row_counter(state, row):
    state['word_count'] += 1
    return state


def reducer_num_of_occurences(rows):
    yield {
        'word': rows[0]['word'],
        'num_of_occurences': len(rows)
    }


def mapper_count_cumul_frequency(row):
    count = row['num_of_occurences']
    total = row['word_count']
    yield {
        'word': row['word'],
        'cumul_frequency': count / total
    }


def reducer_frequency(rows):
    num_words = len(rows)
    doc_id = rows[0]['doc_id']

    word_count = defaultdict(int)
    for row in rows:
        word_count[row['word']] += 1

    for word, count in word_count.items():
        yield {
            'doc_id': doc_id,
            'word': word,
            'frequency': count / num_words
        }


def mapper_index(row):
    freq = row['frequency']
    cumul_freq = row['cumul_frequency']
    yield {
        'doc_id': row['doc_id'],
        'word': row['word'],
        'pmi': math.log(freq / cumul_freq)
    }


if __name__ == '__main__':
    source = '../data/text_corpus.txt'
    # source = '../data/input.txt'
    output = '../data/output.txt'

    split_word = mrop.ComputeGraph() \
        .map(split_input)

    count_words = mrop.ComputeGraph() \
        .input(split_word) \
        .fold(row_counter, {'word_count': 0})

    cumul_frequency = mrop.ComputeGraph() \
        .input(split_word) \
        .sort('word') \
        .reduce(reducer_num_of_occurences, 'word') \
        .join(count_words, strategy='cross') \
        .map(mapper_count_cumul_frequency)

    count_index = mrop.ComputeGraph() \
        .input(split_word) \
        .sort('doc_id') \
        .reduce(reducer_frequency, 'doc_id') \
        .join(cumul_frequency, strategy='inner', key='word') \
        .map(mapper_index)

    count_index.compile()
    count_index.run(
        subgraph_inputs={split_word: open(source, 'r')},
        output=open(output, 'w')
    )
