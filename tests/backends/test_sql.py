import unittest

from bubbles import FieldList, OperationContext
from bubbles.errors import ProbeAssertionError
from bubbles.backends.sql.objects import SQLDataStore
import bubbles.backends.sql.ops

class SQLBackendTestCase(unittest.TestCase):
    def setUp(self):
        self.context = OperationContext()
        self.context.add_operations_from(bubbles.backends.sql.ops)

        self.sql_data_store = SQLDataStore('sqlite:///')
        self.table = self.sql_data_store.create(
            'test',
            FieldList(('a', 'integer'), ('b', 'integer'), ('c', 'integer')),
            replace=True)

        # sample data: a is always 1, c is always unique
        self.data = [(1,2,4), (1,2,3), (1,3,5)]
        self.table.append_from_iterable(self.data)

    def test_field_filter(self):
        result = self.context.op.field_filter(self.table, keep=['a', 'b'])
        self.assertListEqual(['a', 'b'], result.fields.names())

        result = self.context.op.field_filter(self.table, drop=['a', 'b'])
        self.assertListEqual(['c'], result.fields.names())

        result = self.context.op.field_filter(self.table, rename={'c': 'd'})
        self.assertListEqual(['a', 'b', 'd'], result.fields.names())

        # check ordering
        result = self.context.op.field_filter(self.table, keep=['b', 'a'])
        self.assertListEqual(['b', 'a'], result.fields.names())

    def test_filter_by_value(self):
        result = self.context.op.filter_by_value(self.table, 'b', 2)
        self.assertTrue(all(b == 2 for (_, b, _) in result.rows()))
        self.assertEqual(2, len(list(result.rows())))

    def test_filter_by_range(self):
        result = self.context.op.filter_by_range(self.table, 'c', 3, 4)
        self.assertTrue(all(c in range(3, 5) for (_, _, c) in result.rows()))
        self.assertEqual(2, len(list(result.rows())))

    def test_filter_not_empty(self):
        # add a row with a null value for c
        self.table.append_from_iterable([(1,2,None)])

        result = self.context.op.filter_not_empty(self.table, 'c')
        self.assertEqual(3, len(list(result.rows())))

    def test_distinct(self):
        # 3 distinct values of c
        result = self.context.op.distinct(self.table)
        self.assertEqual(3, len(list(result.rows())))

        # 1 distinct value of a
        result = self.context.op.distinct(self.table, ['a'])
        self.assertEqual(1, len(list(result.rows())))

    def test_sample(self):
        result = self.context.op.sample(self.table, 2)
        self.assertEqual(2, len(list(result.rows())))

    def test_sort(self):
        result = self.context.op.sort(self.table, 'a')
        self.assertListEqual(
            sorted(self.data, key=lambda x: x[0]), list(result.rows()))

        result = self.context.op.sort(self.table, 'c')
        self.assertListEqual(
            sorted(self.data, key=lambda x: x[2]), list(result.rows()))

    def test_aggregate(self):
        result = self.context.op.aggregate(self.table, 'b', [('c', 'sum')])
        b_val, c_sum, record_count =list(result.rows())[0]

        self.assertEqual(2, b_val)
        self.assertEqual(sum(c for (_, b, c) in self.data if b == 2), c_sum)
        self.assertEqual(
            len(list(1 for _, b, _ in self.data if b == 2)), record_count)

    def test_assert_unique(self):
        self.context.op.assert_unique(self.table, 'c')

        with self.assertRaises(ProbeAssertionError):
            self.context.op.assert_unique(self.table, 'a')

    def test_assert_contains(self):
        self.context.op.assert_contains(self.table, 'a', 1)

        with self.assertRaises(ProbeAssertionError):
            self.context.op.assert_contains(self.table, 'a', 2)

    def test_assert_missing(self):
        self.context.op.assert_missing(self.table, 'a', 3)

        with self.assertRaises(ProbeAssertionError):
            self.context.op.assert_missing(self.table, 'a', 1)
