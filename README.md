# Computation Graph

This is a library for computation graphs over tables. It allows to define operation sequences as computational graphs. After that, the computational graphs can be launced. They will read data from one file and write the result into a different one. The tables are stored as a sequence of ```dict```-s.

## 1. Operations

Six type of operations are supported: Map, Reduce, Join, Fold, Sort, Input.

### 1.1 Map(mapper)

Calls mapper at each table row. The mapper function must be a generator. It takes a row of the input table and returns a new row or a set of rows.

### 1.2 Reduce(reducer, key)

Groups rows by common keys or groups of keys, and calls reducer at each row group. Reducer function must be a generator which takes a set of rows and returning a row or a set of rows. Before calling Reduce, the table must be sorted by the corresponding key.

### 1.3 Join(graph, key, strategy)

This operation connects the graph which it belongs to with a new graph by a key or a set of keys. There are 5 possible strategies: inner, left, right, full, cross. The description of these operations may be found at https://en.wikipedia.org/wiki/Join_(SQL).

### 1.4 Fold(folder, start_state)

Calls folder at each table row. Folder is a function that takes a state and a row and returns an updated state.

### 1.5 Sort(key)

Sorts the table by a key of a set of keys (lexicographically).

### 1.6 Input(source)

Defines an input for a given graph. The input may be an read-open text file of a different graph. See details below in Interface section.


## 2. Interface

The general scheme of working with the library is as follows.
1) Define a graph.
2) Compile a graph.
3) Launching a graph.

### 2.1 Defining a graph

Each graph is a linear consequence of operations. Performing several operations may depend on different graphs.

There are two ways to define a graph: via ```add``` or "via dot".

Examples:

1) via add 
```python
graph = mrop.ComputeGraph()
graph.add(Map(mapper))
graph.add(Sort('my_key'))
graph.add(Reduce(reducer, 'my_key'))
```

2) "via dot"
```python
count_idf = mrop.ComputeGraph() \
    .input(split_word) \
    .sort(['doc_id', 'word']) \
    .reduce(reducer_unique, ['doc_id', 'word']) \
    .join(count_docs, strategy='cross') \
    .sort('word') \
    .reduce(reducer_calc_idf, 'word')
```

Methods __init__ of classes Map, Reduce, ... take as input the same arguments as methods map, reduce, ... of class ComputeGraph.

*Note concerning Input*. On the contrary to other oerations, input can be defined either before ```compile``` or at the start of computation (```run```). If the input is defined before ```compile```, it cannot be modified at computation (```run```). Moreover, if input is a different graph, it can only be defined before ```compile``` and not before ```run```.

### 2.2 Graph compilation

Compilation should be done before launching the graph, since the order of actions is determined at the stage of compilation. It is done by calling ```compile()``` without arguments. **Parameter *topsort_needed* is not to be used by the user; changing it leads to undefined behavior**.

Graph can be compiled only once. After the compilation new operations cannot be added.

### Launching the graph

Launching is done via ```run()``` method. It is obligatory that inputs for the graph and all the graphs which it is dependent on are defined (parameters input, subgraph_inputs). Input may be either a graph or an opened file. If input is a different graph, it must be defined before ```compile```. If an input is a file, it may be specified both before compile and before run. The inputs defined before compile cannot be redefined during ```run```.

*Note*. If two subgraphs take the same file as input, the library allows to avoid duplicate file reading.

Example which leads to duplicate reading:

```python
source = 'input.txt'
subgraph_sources = {
    split_word: open(source, 'r'),
    count_docs: open(source, 'r')
}
calc_index.run(output=open(output, 'w'), subgraph_inputs=subgraph_sources, verbose=1)
```

Example of calling ```run``` correctly:

```python
source = 'input.txt'
with open(source, 'r') as in_file:
    subgraph_sources = {
        split_word: in_file,
        count_docs: in_file
    }
    calc_index.run(output=open(output, 'w'), subgraph_inputs=subgraph_sources, verbose=1)
```

# Computation Graph (Russian docs version)

Это библиотека для организации вычислений над таблицами. С её помощью можно задавать последовательности операций в виде графа вычислений, а затем запускать полученные графы, считывая данные из одного файла и записывая результат в другой. Таблицы задаются последовательностью словарей dict.

## 1. Операции

Поддерживаются 6 типов операций: Map, Reduce, Join, Fold, Sort, Input.

### 1.1 Map(mapper)

Вызывает функцию mapper от каждой из строк таблицы. Функция mapper должна быть генератором. Она принимает строку исходной таблицы и выдаёт новую строку или набор строк.

### 1.2 Reduce(reducer, key)

Группирует строки по группам с общим ключом или набором ключей key, затем от каждой группы строк вызывает reducer. При этом функция reducer должна быть генератором, принимающим набор строк и выдающим строку или набор строк. Перед вызовом операции Reduce необходимо отсортировать таблицу по соответствубщему ключу key.

### 1.3 Join(graph, key, strategy)

Эта операция соединяет граф, в котором находится, с новым графом graph по ключу(или набору ключей) key. Возможно 5 вариантов, или стратегий: inner, left, right, full, cross. Описание операций можно найти на https://ru.wikipedia.org/wiki/Join_(SQL).

### 1.4 Fold(folder, start_state)

Поледовательно вызывает folder от всех строк таблицы. folder -- функция, которая принимает состояние state и строку row и возвращает обновлённое состояние.

### 1.5 Sort(key)

Сортирует таблицу по ключу или набору ключей key лексикографически.

### 1.6 Input(source)

Задаёт вход для данного графа. Входом может быть открытый на чтение текстовый файл или другой граф. Особенности Input см. ниже в разделе интерфейс, задание графа.


## 2. Интерфейс

Общая схема работы с библиотекой такова:
1) Задать граф
2) Скомпилировать граф
3) Запустить граф

### 2.1 Задание графа

Каждый граф есть линейная последовательность операций. При этом выполнение некоторых операций может зависеть от других графов. 

Есть два способа задавать граф: с помощью метода add и с "через точку". Примеры:

1) Через add 
```python
graph = mrop.ComputeGraph()
graph.add(Map(mapper))
graph.add(Sort('my_key'))
graph.add(Reduce(reducer, 'my_key'))
```

2) "Через точку"
```python
count_idf = mrop.ComputeGraph() \
    .input(split_word) \
    .sort(['doc_id', 'word']) \
    .reduce(reducer_unique, ['doc_id', 'word']) \
    .join(count_docs, strategy='cross') \
    .sort('word') \
    .reduce(reducer_calc_idf, 'word')
```

Методы __init__ классов Map, Reduce, ... принимают на вход то же, что и методы map, reduce, ... класса ComputeGraph.

Примечание об Input. В отличие от других операций, вход можно задавать как перед компиляцией (compile), так и во время начала вычислений (run). Если вход задаётся перед compile, то на этапе run его уже нельзя изменить. Кроме того, если входом является другой граф, то его можно задать только перед compile, а при вызове run нельзя.

### 2.2 Компиляция графа

Необходимо провести провести перед запуском графа, т.к. во время компиляции определяется порядок действий. Производится с помощью вызова compile() без аргументов. ПАРАМЕТР topsort_needed НЕ ПРЕДНАЗНАЧЕН ДЛЯ ИСПОЛЬЗОВАНИЯ ПОЛЬЗОВАТЕЛЕМ, И ЕГО ИЗМЕНЕНИЕ ПРИВЕДЁТ К НЕОПРЕДЕЛЁННОМУ ПОВЕДЕНИЮ.

Граф можно скомпилировать только один раз. После компиляции нельзя добавлять новые операции.

### Запуск графа

Вызвается с помощью метода run(). Для вычисления необоходимо, чтобы для графа и для всех графов, от которых он зависит, были заданы входы (параметры input, subgraph_inputs). Входом может быть другой граф или же открытый файл. Если входом является другой граф, его необходимо задать до compile. Если входом является файл, то его можно задать и до compile, и во время run. При этом входы, заданные до compile, нельзя перезадать во время run.

Примечание. Если у двух подграфов входом является один и тот же один и тот же файл (именно файл, а не граф), то библиотека позволяет избежать повторного чтения.

Пример вызова, который приведёт к повторному чтению:

```python
source = 'input.txt'
subgraph_sources = {
    split_word: open(source, 'r'),
    count_docs: open(source, 'r')
}
calc_index.run(output=open(output, 'w'), subgraph_inputs=subgraph_sources, verbose=1)
```

Пример правильного вызова:

```python
source = 'input.txt'
with open(source, 'r') as in_file:
    subgraph_sources = {
        split_word: in_file,
        count_docs: in_file
    }
    calc_index.run(output=open(output, 'w'), subgraph_inputs=subgraph_sources, verbose=1)
```
