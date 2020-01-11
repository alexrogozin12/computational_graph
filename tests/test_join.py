import sys

sys.path.append('../')
import mrop

first_data = [
    {'col_A': '1', 'col_B': 'a'},
    {'col_A': '2', 'col_B': 'b'},
    {'col_A': '3', 'col_B': 'c'},
    {'col_A': '4', 'col_B': 'd'},
    {'col_A': '5', 'col_B': 'x'},
    {'col_A': '6', 'col_B': 'y'}
]

second_data = [
    {'col_B': 'a', 'col_C': 'A'},
    {'col_B': 'b', 'col_C': 'B'},
    {'col_B': 'c', 'col_C': 'C'},
    {'col_B': 'd', 'col_C': 'D'},
    {'col_B': 'e', 'col_C': 'E'},
    {'col_B': 'f', 'col_C': 'F'}
]

inner_res = [
    {'col_A': '1', 'col_B': 'a', 'col_C': 'A'},
    {'col_A': '2', 'col_B': 'b', 'col_C': 'B'},
    {'col_A': '3', 'col_B': 'c', 'col_C': 'C'},
    {'col_A': '4', 'col_B': 'd', 'col_C': 'D'}
]

left_res = [
    {'col_A': '1', 'col_B': 'a', 'col_C': 'A'},
    {'col_A': '2', 'col_B': 'b', 'col_C': 'B'},
    {'col_A': '3', 'col_B': 'c', 'col_C': 'C'},
    {'col_A': '4', 'col_B': 'd', 'col_C': 'D'},
    {'col_A': '5', 'col_B': 'x', 'col_C': None},
    {'col_A': '6', 'col_B': 'y', 'col_C': None},
]

right_res = [
    {'col_A': '1', 'col_B': 'a', 'col_C': 'A'},
    {'col_A': '2', 'col_B': 'b', 'col_C': 'B'},
    {'col_A': '3', 'col_B': 'c', 'col_C': 'C'},
    {'col_A': '4', 'col_B': 'd', 'col_C': 'D'},
    {'col_A': None, 'col_B': 'e', 'col_C': 'E'},
    {'col_A': None, 'col_B': 'f', 'col_C': 'F'}
]

second = mrop.ComputeGraph()
second.source = second_data

first_inner = mrop.ComputeGraph()
first_inner.source = first_data
first_inner.add(mrop.Join(second, strategy='inner', key='col_B'))
first_inner.compile()
first_inner.compute()

first_left = mrop.ComputeGraph()
first_left.source = first_data
first_left.add(mrop.Join(second, strategy='left', key='col_B'))
first_left.compile()
first_left.compute()

first_right = mrop.ComputeGraph()
first_right.source = first_data
first_right.add(mrop.Join(second, strategy='right', key='col_B'))
first_right.compile()
first_right.compute()


def test_inner():
    assert first_inner.result == inner_res


def test_left():
    assert first_left.result == left_res


def test_right():
    assert first_right.result == right_res
