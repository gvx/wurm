from dataclasses import dataclass
from datetime import date, time, datetime
from pathlib import Path

import pytest

import wurm

import sqlite3

@pytest.fixture
def connection():
    token = wurm.setup_connection(sqlite3.connect(":memory:"))
    yield
    wurm.close_connection(token)


@dataclass
class Point(wurm.Table):
    x: int
    y: int

@dataclass
class Datatypes(wurm.Table):
    string: str
    blob: bytes
    i: int
    f: float
    boolean: bool
    d: date
    t: time
    dt: datetime
    path: Path

@dataclass
class UniqueInt(wurm.Table):
    x: wurm.Unique[int]

@dataclass
class SomeAbstractTable(wurm.Table, abstract=True):
    foo: str

from wurm.typemaps import Annotated

@dataclass
class NonUniqueInt(wurm.Table):
    x: Annotated[int, {'test': True}]

@dataclass
class NoRowid(wurm.WithoutRowid):
    key: wurm.Primary[str]
    count: int

@dataclass
class CompositeKey(wurm.WithoutRowid):
    part_one: wurm.Primary[int]
    part_two: wurm.Primary[int]

@dataclass
class ForeignKeyTest(wurm.Table):
    point: Point
    word: NoRowid

@dataclass
class OneToOneForeignKeyTest(wurm.Table):
    point: wurm.Unique[Point]

@dataclass
class PrimaryForeignKeyTest(wurm.WithoutRowid):
    point: wurm.Primary[Point]

@wurm.register_dataclass
@dataclass
class Color:
    r: float
    g: float
    b: float
    a: float

@dataclass
class MultiColumnFields(wurm.Table):
    color: Color

@dataclass
class ForeignKeyTest2(wurm.Table):
    point: CompositeKey

def test_no_connection():
    with pytest.raises(wurm.WurmError, match=r'setup_connection\(\) not called'):
        Point(0,0).insert()

def test_model(connection):
    assert Point.__table_name__ == 'Point'

def test_model_empty(connection):
    assert len(Point) == 0

def test_can_iter(connection):
    assert list(Point) == []

def test_can_insert(connection):
    p = Point(10, 20)
    p.insert()
    assert p.rowid is not None

def test_model_not_empty_after_insert(connection):
    p = Point(10, 20)
    p.insert()
    assert len(Point)

def test_model_iter_not_empty(connection):
    p = Point(10, 20)
    p.insert()
    assert list(Point) == [p]
    assert p.rowid == list(Point)[0].rowid

def test_model_eq(connection):
    assert Point(10, 20) == Point(10, 20)

def test_model_eq_norowid(connection):
    p1 = Point(10, 20)
    p1.insert()
    p2 = Point(10, 20)
    p2.insert()
    assert p1 == p2 and p1.rowid != p2.rowid

def test_model_neq(connection):
    assert Point(10, 20) != Point(20, 10)

def test_model_get(connection):
    p1 = Point(10, 20)
    p1.insert()
    assert Point.query(rowid=p1.rowid).one().rowid == p1.rowid

def test_model_get_doesnt_cache(connection):
    p1 = Point(10, 20)
    p1.insert()
    assert Point.query(rowid=p1.rowid).one() is p1

def test_model_update(connection):
    p1 = Point(10, 20)
    p1.insert()
    p1.y = 1000
    p1.commit()
    del p1
    assert Point.query().one().y == 1000

def test_model_delete1(connection):
    p1 = Point(10, 20)
    p1.insert()
    p2 = Point(20, 10)
    p2.insert()
    p1.delete()
    assert len(Point) == 1

def test_model_delete2(connection):
    p1 = Point(10, 20)
    p1.insert()
    p2 = Point(20, 10)
    p2.insert()
    Point.query(rowid=1).delete()
    assert len(Point) == 1

def test_model_delete_no_rowid(connection):
    p1 = Point(10, 20)
    p1.insert()
    p1.delete()
    assert p1.rowid is None

def test_model_delete_all(connection):
    Point(10, 20).insert()
    Point(10, 0).insert()
    Point(0, 20).insert()
    Point(0, 0).insert()
    for p in Point:
        p.delete()
    assert len(Point) == 0

def test_insert_predefined_rowid(connection):
    p = Point(0, 0)
    p.rowid = 7
    p.insert()
    assert next(iter(Point)).rowid == 7

def test_create_table_after_connection(connection):
    @dataclass
    class Strings(wurm.Table):
        s: str
    with pytest.raises(wurm.WurmError):
        len(Strings)

def test_insert_None(connection):
    p = Point(1, None)
    p.insert()
    assert Point.query(rowid=p.rowid).one().y is None

def test_cannot_insert_same_rowid(connection):
    p = Point(0, 0)
    p.rowid = 7
    p.insert()
    p2 = Point(0, 1)
    p2.rowid = 7
    with pytest.raises(wurm.WurmError):
        p2.insert()

def test_datatypes(connection):
    one = Datatypes(string='string',
        blob=b'blob',
        i=0xDEADBEEF,
        f=42.1,
        boolean=True,
        d=date(2021, 1, 9),
        t=time(7, 20, 0),
        dt=datetime(2021, 1, 9, 7, 20, 0),
        path=Path('/var/www/'))
    one.insert()
    assert Datatypes.query(rowid=one.rowid).one() == one

def test_unique(connection):
    UniqueInt(42).insert()
    UniqueInt(7).insert()
    with pytest.raises(wurm.WurmError):
        UniqueInt(42).insert()
    assert UniqueInt.query(rowid=1).one()

def test_other_annotated(connection):
    NonUniqueInt(42).insert()
    NonUniqueInt(42).insert()

def test_query_len_1(connection):
    assert len(Point.query()) == 0

def test_query_len_2(connection):
    Point(0, 0).insert()
    Point(0, 0).insert()
    assert len(Point.query()) == 2

def test_query_len_3(connection):
    Point(0, 0).insert()
    Point(0, 1).insert()
    assert len(Point.query(y=1)) == 1

def test_query_len_4(connection):
    Point(0, 0).insert()
    Point(0, 1).insert()
    Point(0, 2).insert()
    assert len(Point.query(x=0, y=wurm.ge(1))) == 2

def test_query_len_5(connection):
    Point(0, 0).insert()
    assert len(Point.query(y=wurm.gt(1), x=wurm.lt(1))) == 0

def test_query_len_6(connection):
    Point(0, 0).insert()
    assert len(Point.query(y=wurm.le(1), x=wurm.ne(1))) == 1

def test_query_len_7(connection):
    with pytest.raises(wurm.WurmError):
        Point.query(z=0)

def test_query_iter_1(connection):
    p1 = Point(0, 0)
    p1.insert()
    p2 = Point(0, 1)
    p2.insert()
    Point(1, 0).insert()
    assert list(Point.query(x=0)) == [p1, p2]

def test_query_first_1(connection):
    Point(0, 0).insert()
    Point(0, 1).insert()
    assert Point.query(x=0).first().x == 0

def test_query_first_2(connection):
    with pytest.raises(wurm.WurmError):
        Point.query(x=0).first()

def test_query_one_1(connection):
    Point(0, 0).insert()
    Point(0, 1).insert()
    assert Point.query(y=0).one().x == 0

def test_query_one_2(connection):
    Point(0, 0).insert()
    Point(0, 1).insert()
    with pytest.raises(wurm.WurmError):
        Point.query(x=0).one()

def test_query_delete(connection):
    Point(0, 0).insert()
    Point(1, 1).insert()
    Point(1, 2).insert()
    Point.query(y=wurm.lt(2)).delete()
    assert len(Point) == 1

def test_query_delete_all(connection):
    Point(10, 20).insert()
    Point(10, 0).insert()
    Point(0, 20).insert()
    Point(0, 0).insert()
    Point.query().delete()
    assert len(Point) == 0

def test_query_delete_no_rowid(connection):
    with pytest.raises(ValueError):
        Point(0, 0).delete()

def test_table_truthy_without_connection():
    assert Point

def test_abstract_table(connection):
    with pytest.raises(TypeError):
        SomeAbstractTable(foo='hello')

def test_norowid(connection):
    NoRowid('one', 1).insert()
    NoRowid('two', 2).insert()
    NoRowid('three', 3).insert()
    two = NoRowid.query(key='two').one()
    two.count = 42
    two.commit()
    assert len(NoRowid) == 3

def test_no_subclass_nonabstract_table():
    with pytest.raises(TypeError):
        @dataclass
        class Point3D(Point):
            z: int

def test_composite_key(connection):
    CompositeKey(1, 2).insert()
    CompositeKey(2, 1).insert()
    CompositeKey(1, 1).insert()
    with pytest.raises(wurm.WurmError):
        CompositeKey(1, 2).insert()

def test_foreign_keys_1(connection):
    p = Point(1,2)
    p.insert()
    n = NoRowid('ok', 1)
    n.insert()
    fk = ForeignKeyTest(p, n)
    fk.insert()
    assert isinstance(ForeignKeyTest.query(rowid=1).one().point, Point)

def test_foreign_keys_2(connection):
    c = CompositeKey(1, 2)
    c.insert()
    ForeignKeyTest2(c).insert()
    assert ForeignKeyTest2.query().one().point.part_one == 1

def test_foreign_keys_3(connection):
    p = Point(1, 1)
    p.insert()
    OneToOneForeignKeyTest(p).insert()
    p2 = Point(1, 1)
    p2.insert()
    OneToOneForeignKeyTest(p2).insert()
    dup = OneToOneForeignKeyTest(p)
    with pytest.raises(wurm.WurmError):
        dup.insert()

def test_foreign_keys_4(connection):
    p = Point(1, 1)
    p.insert()
    PrimaryForeignKeyTest(p).insert()
    p2 = Point(1, 1)
    p2.insert()
    PrimaryForeignKeyTest(p2).insert()
    dup = PrimaryForeignKeyTest(p)
    with pytest.raises(wurm.WurmError):
        dup.insert()

def test_multicolumnfields(connection):
    MultiColumnFields(Color(0., 2., 8., 1.)).insert()
    o, = MultiColumnFields
    assert o.color == Color(0., 2., 8., 1.)

def test_register_tuple_type():
    class Test:
        pass
    wurm.register_type(Test, (str, int), encode=..., decode=...)
    from wurm import typemaps
    assert list(typemaps.columns_for('field', Test)) == ['field_0',
        'field_1']
