import pytest
import dbldatagen as dg


class TestTopologicalSort:

    def test_sort(self):
        src = [
            ('code3', ['code1', '_r_code3']),
            ('code1', ['id']),
            ('code2', ['code1', '_r_code1']),
            ('id', []),
            ('code3a', []),
            ('_r_code1', []),
            ('_r_code3', [])
        ]

        output = list(dg.topologicalSort(src))
        print("output1", output)

        assert output == ['id', 'code3a', '_r_code1', '_r_code3', 'code1', 'code3', 'code2']

    def test_sort2(self):
        src = [
            ('code3', ['code1', '_r_code3']),
            ('code1', ['id']),
            ('code2', ['code1', '_r_code1', 'id']),
            ('id', []),
            ('code3a', []),
            ('_r_code1', ['id']),
            ('_r_code3', ['id'])
        ]

        output = list(dg.topologicalSort(src, initial_columns=['id'], flatten=False))

        print("output1", output)

        assert output == [['id'], ['code1', 'code3a', '_r_code1', '_r_code3'], ['code3', 'code2']]

    def test_empty_list(self):
        src = []
        output = list(dg.topologicalSort(src))
        print("output", output)

        assert output == []

    def test_singleton_list(self):
        src = [('id', [])]
        output = list(dg.topologicalSort(src))
        print("output", output)

        assert output ==  ['id']
