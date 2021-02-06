from dataclasses import dataclass
import sqlite3

import wurm
import pytest


@pytest.fixture
def connection():
    token = wurm.setup_connection(sqlite3.connect(":memory:"))
    yield
    wurm.close_connection(token)

@dataclass
class Parent(wurm.Table):
    children = wurm.relation('Child.parent')

@dataclass
class Child(wurm.Table):
    parent: Parent

@dataclass
class Parent2(wurm.Table):
    children = wurm.relation('Child2')

@dataclass
class Child2(wurm.Table):
    parent1: Parent2
    parent2: Parent2

@dataclass
class WrongRelation(wurm.Table):
    oh_no = wurm.relation('test_relation_1')

@dataclass
class Parent3(wurm.Table):
    children = wurm.relation('Child')

@dataclass
class Parent4(wurm.Table):
    children = wurm.relation('Child4')

@dataclass
class Child4(wurm.Table):
    parent: Parent4

@dataclass
class Parent5(wurm.Table):
    children = wurm.relation('Child.parent')

@dataclass
class Parent6(wurm.Table):
    children = wurm.relation('Child6.parent', lazy='query')

@dataclass
class Child6(wurm.Table):
    parent: Parent6

@dataclass
class Parent7(wurm.Table):
    children = wurm.relation('Child7.parent', lazy='strict')

@dataclass
class Child7(wurm.Table):
    parent: Parent7

def test_relation_1(connection):
    p = Parent()
    p.insert()
    Child(parent=p).insert()
    Child(parent=p).insert()
    Child(parent=p).insert()
    assert len(p.children) == 3

def test_ambiguous_relation():
    with pytest.raises(TypeError, match='multiple Parent2 fields'):
        Parent2().children

def test_wrong_relation_type():
    with pytest.raises(TypeError, match='invalid target'):
        WrongRelation().oh_no

def test_relation_no_relevant_field():
    with pytest.raises(TypeError, match='does not have a Parent3 field'):
        Parent3().children

def test_relation_2(connection):
    p = Parent4()
    p.insert()
    Child4(parent=p).insert()
    Child4(parent=p).insert()
    Child4(parent=p).insert()
    assert len(p.children) == 3

def test_relation_field_wrong_type():
    with pytest.raises(TypeError, match=r'Child\.parent is not Parent5'):
        Parent5().children

def test_relation_on_class():
    # FIXME: what should this do?
    assert Parent.children.target == 'Child.parent'

def test_access_relation_twice(connection):
    p = Parent()
    p.insert()
    Child(parent=p).insert()
    Child(parent=p).insert()
    Child(parent=p).insert()
    assert len(p.children) == len(p.children)

def test_relation_3(connection):
    assert isinstance(Parent6().children, wurm.Query)

def test_relation_4(connection):
    p = Parent7()
    p.insert()
    Child7(parent=p).insert()
    Child7(parent=p).insert()
    Child7(parent=p).insert()
    assert len(p.children) == 0

def test_relation_5(connection):
    p = Parent7()
    p.insert()
    p, = Parent7
    Child7(parent=p).insert()
    Child7(parent=p).insert()
    Child7(parent=p).insert()
    assert len(p.children) == 0


def test_relation_6(connection):
    p = Parent7()
    p.insert()
    Parent7.del_object(p) # remove object from identity mapping
    p, = Parent7
    Child7(parent=p).insert()
    Child7(parent=p).insert()
    Child7(parent=p).insert()
    assert len(p.children) == 0
