import json


class ComputeGraph(object):
    '''
    Вычислительный граф. Используется для вычислений над таблицами.
    Таблицы задаются как последовательность словарей.
    Поддерживаемые операции: Input, Map, Reduce, Sort, Join, Fold.
    Добавление операции: graph.add(Map(mapper)) или graph.map(mapper)
    Перед вычислением графа его нужно скомпилировать, вызвав compile
    Выполнение вычислений -- run
    '''

    def __init__(self):
        '''
        Инициализирует вычислительный граф.
        '''
        self.operations = []
        self.result = None
        self.source = None
        self.dependencies = []
        self.compiled = False
        self.compute_order = []

    def compile(self, topsort_needed=True):
        '''
        Компилирует граф, после чего тот становится готов к запуску вычислений.
        :param topsort_needed: bool, провести ли топологическую сортировку зависимостей
        графа для определения порядка вычислений.
        :return: None
        '''
        # print('compiling graph {}...'.format(self))

        if self.compiled:
            raise AttributeError('Graph already compiled')

        if topsort_needed:
            visited_vertices = set()
            self.update_compute_order(self.compute_order, visited_vertices)
            del self.compute_order[-1]
            for graph in self.compute_order:
                if not graph.compiled:
                    graph.compile(topsort_needed=False)

        if len(self.operations) == 0:
            self.compiled = True
            return
        if self.source is not None:
            self.operations[0].set_input_gen(self.source)

        for (i, op) in enumerate(self.operations):
            if i == 0:
                continue
            op.set_input_gen(self.operations[i - 1])

        self.compiled = True

    def compute(self, verbose=0):
        '''
        Проводит вычисления и записывает результат в self.result.
        :param verbose: если 1, то выводить информацию о ходе выполнения
        :return: None
        '''
        if verbose:
            print('computing graph {}'.format(self))
        if not self.compiled:
            raise AttributeError('Graph should be compiled before compute')
        if self.source is None:
            raise AttributeError('Cannot compute graph. Input not specified')

        for graph in self.compute_order:
            graph.compute(verbose=verbose)

        if len(self.operations) == 0:
            self.result = list(self.source)
            return

        self.operations[0].set_input_gen(self.source)
        self.result = list(self.operations[-1])
        self.operations[0].del_input_gen()

    def update_compute_order(self, compute_order, visited_vertices):
        '''
        Используется для обхода в глубину при топологической сортировке.
        :param compute_order: list, текущий порядок вычисления подграфов.
        :param visited_vertices: set, посещённые подграфы
        :return: None
        '''
        for subgraph in self.dependencies:
            if subgraph not in visited_vertices:
                subgraph.update_compute_order(compute_order, visited_vertices)
        compute_order.append(self)
        visited_vertices.add(self)

    def set_subgraph_inputs(self, subgraph_inputs):
        '''
        Сопоставить каждому подграфу его вход.
        :param subgraph_inputs: dict вида {subgraph: input}
        :return: None
        '''
        # print('subgraph_inputs = {}'.format(subgraph_inputs))
        if subgraph_inputs is None:
            return
        for input in subgraph_inputs.values():
            if isinstance(input, ComputeGraph):
                raise AttributeError('If input is a graph, it must be specified '
                                     'at the stage of compilation')

        for graph in self.compute_order:
            if graph.source is not None and \
                            graph in subgraph_inputs:
                raise AttributeError('Input for subgraph {} already specified'
                                     .format(graph))
            if graph.source is None and \
                            graph not in subgraph_inputs:
                raise AttributeError('Input for subgraph {} not specified'
                                     .format(graph))

        subgraph_input_frames = {input: Input(input)
                                 for input in set(subgraph_inputs.values())}
        for graph in subgraph_inputs:
            graph.source = subgraph_input_frames[subgraph_inputs[graph]]

    def del_subgraph_inputs(self, subgraph_inputs):
        '''
        Удаляет входы подграфов после окончания вычислений,
        чтобы граф можно было запустить ещё раз.
        :param subgraph_inputs: Словарь вида {graph: input}.
        Сопоставляет входные файлы подграфам.
        :return: None
        '''
        if subgraph_inputs is None:
            return
        for graph in subgraph_inputs:
            graph.source = None

    def run(self, *, input=None, output=None, subgraph_inputs=None, verbose=0):
        '''
        Выполнить вычисления, заданные в графе и сохранить результат в output.
        На этапе run можно задать входы только из файлов. Если входом является
        другой граф, то такой вход нужно задать перед compile
        :param input: Открытый на чтение входной файл.
        :param output: Открытый на запись выходной файл.
        :param subgraph_inputs: Открытые на чтение входные файлы для подграфов.
        :param verbose: если 1, то выводить информацию о ходе выполнения
        Важно: чтобы один и тот же входной файл не читался несколько раз, нужно
        передать результат одного и того же open в несколько подграфов.
        :return: None
        '''
        if input is None and self.source is None:
            raise ValueError('Input not specified')
        elif input is not None and self.source is not None:
            raise ValueError('Input already specified')
        elif isinstance(input, ComputeGraph):
            raise ValueError('If input is another graph, '
                             'it must be specified before compile')
        elif self.source is None:
            self.source = Input(input)

        if output is None:
            raise ValueError('Output not specified')

        self.set_subgraph_inputs(subgraph_inputs)
        self.compute(verbose=verbose)
        self.del_subgraph_inputs(subgraph_inputs)

        buf = '\n'.join([json.dumps(line) for line in self.result])
        output.write(buf)

    def add(self, operation):
        '''
        Добавляет операцию в конец списка операций графа.
        :param operation: Operation, добавляемая операция
        :return: None
        '''
        if self.compiled:
            raise AttributeError('Cannot add operation, graph already compiled')
        if isinstance(operation, Input):
            if len(self.operations) > 0:
                raise ValueError('Input must be specified as first operation')
            if self.source is not None:
                raise ValueError('Input already specified')

            self.source = operation
            if isinstance(operation.data_from, ComputeGraph):
                self.dependencies.append(operation.data_from)
        else:
            self.operations.append(operation)
            if isinstance(operation, Join):
                self.dependencies.append(operation.graph)

    def input(self, input):
        '''
        Задать вход для графа.
        :param input: Input
        :return: self
        '''
        self.add(Input(input))
        return self

    def map(self, mapper):
        '''
        Добавить операцию Map.
        :param mapper: generator, используемый маппер.
        :return: self
        '''
        self.add(Map(mapper))
        return self

    def sort(self, key):
        '''
        Добавить операцию Sort.
        :param key: string или list of strings, набор ключей
        :return: self
        '''
        self.add(Sort(key))
        return self

    def fold(self, folder, start_state):
        '''
        Добавить операцию Fold.
        :param folder: callable, используемый фолдер.
        :param start_state: Начальное состояние
        :return: self
        '''
        self.add(Fold(folder, start_state))
        return self

    def reduce(self, reducer, key):
        '''
        Добавить операцию Reduce.
        :param reducer: generator, используемый редьюсер.
        :param key: string или list of strings, набор ключей
        :return: self
        '''
        self.add(Reduce(reducer, key))
        return self

    def join(self, graph, key=None, strategy='inner'):
        '''
        Добавить операцию Join
        :param graph: ComputeGraph, граф, с которым выполняется Join
        :param key: string или list of strings, набор ключей
        :param strategy: используемая стратегия: inner, left, right, full, cross
        :return: self
        '''
        self.add(Join(graph, key, strategy))
        return self


class Operation(object):
    '''
    Операция, выполняемая в вычислительном графе. Может быть одной из
    Input, Map, Reduce, Sort, Join, Fold. Каждая операция имеет входной и
    выходной генераторы. Когда операция вызывается, она достаёт из
    входного генератора строку или строки, выполняет некоторое действие.
    Следующая операция может получить результат из выходного генератора
    данной.
    '''

    def __init__(self):
        '''
        Инициализирует операцию.
        '''
        self.input_gen = None

    def set_input_gen(self, it):
        '''
        Задаёт входной генератор.
        :param it: iterable, генератор
        :return: None
        '''
        self.input_gen = it

    def del_input_gen(self):
        '''
        Удаляет входной генератор. Используется для того, чтобы граф
        можно было запускать несколько раз.
        :return: None
        '''
        self.input_gen = None


class Input(Operation):
    '''
    Задаёт вход вычислительного графа. Входом может быть открытый файл или
    другой вычислительный граф.
    '''

    def __init__(self, source):
        '''
        Устанавливает source(ComputeGraph или открытый на чтение файл) -- источник информации.
        :param source:
        '''
        super().__init__()
        self.data_from = source
        self.data = None

    def __iter__(self):
        '''
        Выходной генератор (см. документацию Operation).
        :return: None
        '''
        if not isinstance(self.data_from, ComputeGraph) and self.data is None:
            # print('Reading from file {}'.format(self.data_from.name))
            buf = self.data_from.read()
            self.data = [json.loads(line)
                         for line in buf.split('\n')
                         if len(line) > 0]
        elif isinstance(self.data_from, ComputeGraph):
            self.data = self.data_from.result

        for line in self.data:
            yield line


class Map(Operation):
    def __init__(self, mapper):
        super().__init__()
        self.mapper = mapper

    def __iter__(self):
        '''
        Выходной генератор (см. документацию Operation).
        :return: None
        '''
        for item in self.input_gen:
            yield from self.mapper(item)


class Sort(Operation):
    def __init__(self, key):
        super().__init__()
        self.key = key

    def __iter__(self):
        '''
        Выходной генератор (см. документацию Operation).
        :return: None
        '''
        if not isinstance(self.key, list):
            self.key = [self.key]
        comparator = lambda dct: [dct[item] for item in self.key]
        table = sorted(self.input_gen, key=comparator)
        for item in table:
            yield item
        del table


class Fold(Operation):
    def __init__(self, folder, start_state):
        super().__init__()
        self.folder = folder
        self.state = start_state

    def __iter__(self):
        '''
        Выходной генератор (см. документацию Operation).
        :return: None
        '''
        for item in self.input_gen:
            self.state = self.folder(self.state, item)
        yield self.state


class Reduce(Operation):
    def __init__(self, reducer, key):
        super().__init__()
        self.reducer = reducer
        if not isinstance(key, list):
            self.key = [key]
        else:
            self.key = key

        self.get_key = lambda dct: [dct[item] for item in self.key]

    def __iter__(self):
        '''
        Выходной генератор (см. документацию Operation).
        :return: None
        '''
        self.buf = []
        for item in self.input_gen:
            if len(self.buf) == 0:
                self.buf.append(item)
            elif self.get_key(item) == self.get_key(self.buf[0]):
                self.buf.append(item)
            else:
                yield from self.reducer(self.buf)
                self.buf.clear()
                self.buf.append(item)

        if len(self.buf) == 0:
            raise ValueError('Reducer buf cannot be empty')
        yield from self.reducer(self.buf)


class Join(Operation):
    def __init__(self, graph, key=None, strategy='inner'):
        '''
        Устанавливает граф, с которым производится Join, набор ключей и
        стратегию соединения (inner, left, right, full, cross).
        :param graph: граф, с которым производится Join
        :param key: набор ключей
        :param strategy: тип соединения (inner, left, right, full, cross)
        '''
        self.strategies = {
            'inner': self.join,
            'left': self.join,
            'right': self.join,
            'full': self.join,
            'cross': self.cross
        }
        if strategy not in self.strategies.keys():
            raise ValueError('Unknown strategy: {}.\nPlease specify one of {}'
                             .format(strategy, self.strategies))
        super().__init__()
        self.graph = graph
        self.key = key
        self.strategy = strategy

    def dict_slice(self, dct, keys):
        '''
        Выбирает ключи и соответствующие им значения из словаря.
        :param dct: словарь
        :param keys: ключи
        :return: dict
        '''
        # print('dict_slice, keys = {}'.format(keys))
        return {key: dct[key] for key in keys}

    def get_monokey_piece(self, table, key, start):
        '''
        Выбирает из таблицы кусок максимальной длины, начинающийся со start
        и состоящий из строк с одинаковым значением ключа key.
        :param table: таблица
        :param key: string или list of strings
        :param start: индекс начала
        :return: stop -- конец куска
        val -- значение ключа, одинаковое для всего куска
        '''
        if start >= len(table):
            raise IndexError('Start index out of table range')
        val = self.dict_slice(table[start], key)
        stop = start
        while stop < len(table) and self.dict_slice(table[stop], key) == val:
            stop += 1
        return stop, val

    def cross(self):
        '''
        Производит cross join. Является выходном генератором(см. документацию Operation).
        :return: None
        '''
        if self.key is not None:
            raise ValueError('Cross join does not require a key')
        for self_item in self.input_gen:
            for new_item in self.graph.result:
                line = self_item.copy()
                line.update(new_item)
                yield line

    def join(self):
        '''
        Производит inner, left outer, right outer или full outer join в
        зависимости от выбранной стратегии.
        :return: None
        '''
        if self.key is None:
            raise ValueError('Key for join must not be None')
        if not isinstance(self.key, list):
            self.key = [self.key]

        sorter = Sort(self.key)
        sorter.set_input_gen(self.input_gen)
        self_table = list(sorter)

        # print('self_table:')
        # for line in self_table:
        #     print(line)

        sorter.set_input_gen(self.graph.result)
        new_table = list(sorter)

        # print('new_table:')
        # for line in new_table:
        #     print(line)

        # with open('./tests/self_table_dump.txt', 'w') as f:
        #     buf = '\n'.join([json.dumps(line) for line in self_table])
        #     f.write(buf)
        # with open('./tests/new_table_dump.txt', 'w') as f:
        #     buf = '\n'.join([json.dumps(line) for line in new_table])
        #     f.write(buf)

        self_start, self_stop = 0, 0
        new_start, new_stop = 0, 0
        while self_start < len(self_table):
            self_start = self_stop
            if self_start == len(self_table):
                break
            self_stop, self_val = self.get_monokey_piece(self_table, self.key, self_start)
            new_val = {}
            while new_start < len(new_table) and new_val != self_val:
                new_start = new_stop
                if new_start == len(new_table):
                    # print('new_start = len(new_table)')
                    break
                new_stop, new_val = self.get_monokey_piece(new_table, self.key, new_start)
                if new_val != self_val and self.strategy in ['right', 'full']:
                    for new_row in new_table[new_start:new_stop]:
                        line = {key: None for key in self_table[0]}
                        line.update(new_row)
                        yield line

            if new_start == len(new_table) and self.strategy in ['left', 'full']:
                for self_row in self_table[self_start:self_stop]:
                    line = {key: None for key in new_table[0]}
                    line.update(self_row)
                    yield line
                continue

            if new_start < len(new_table):
                for self_row in self_table[self_start:self_stop]:
                    for new_row in new_table[new_start:new_stop]:
                        line = self_row.copy()
                        line.update(new_row)
                        yield line

    def __iter__(self):
        '''
        Выходной генератор (см. документацию Operation). Конкретный генератор зависит от стратегии.
        :return: None
        '''
        # print('Join with {}'.format(self.graph.result))
        return self.strategies[self.strategy]()
