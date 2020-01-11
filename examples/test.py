import mrop
import re
import string


def split_text(row):
    # print(row['text'])
    words = row['text'].split()

    characters_to_exclude = set(string.punctuation)

    for word in words:
        word = "".join(ch for ch in word.lower()
                       if ch not in characters_to_exclude)
        if len(word) > 0:
            yield {
                'doc_id': row['doc_id'],
                'word': word
            }

            # yield {
            #     'doc_id' : row['doc_id'],
            #     'word' : "".join(ch for ch in word.lower()
            #                      if ch not in characters_to_exclude)
            # }


def split_input(row):
    words = re.split(r'[ \n\t,;.-]+', row['text'])
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


def mapper_new(row):
    new_row = {'res': row['start'][0] + row['end'][0]}
    new_row = {'res': 10}
    yield new_row


def reducer_new(rows):
    values = [row['res'] for row in rows]
    yield {'final_res': sum(values)}


if __name__ == '__main__':
    # source = '../data/input.txt'
    source = '../data/text_corpus.txt'

    semi_graph = mrop.ComputeGraph()
    semi_graph.add(mrop.Input(open('../data/one_str.txt', 'r')))
    # semi_graph.compile()

    graph = mrop.ComputeGraph()
    graph.add(mrop.Input(open(source, 'r')))
    # graph.add(mrop.Input(open('./tests/input.txt', 'r')))
    # graph.add(mrop.Map(split_input))
    graph.add(mrop.Map(split_text))
    graph.add(mrop.Sort('word'))
    graph.add(mrop.Reduce(word_counter, 'word'))
    graph.add(mrop.Join(semi_graph, strategy='cross'))

    graph.compile()
    graph.run(output=open('../data/output.txt', 'w'))
