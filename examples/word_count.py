import mrop
import re


def split_input(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


def word_counter(rows):
    word = rows[0]['word']
    word_num = len(rows)
    yield {
        'word': word,
        'word_num': word_num
    }


if __name__ == '__main__':
    # source = '../data/input.txt'
    source = '../data/text_corpus.txt'

    graph = mrop.ComputeGraph() \
        .map(split_input) \
        .sort('word') \
        .reduce(word_counter, 'word')

    graph.compile()
    graph.run(input=open(source, 'r'), output=open('../data/output.txt', 'w'))
