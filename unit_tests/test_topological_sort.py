import unittest
import databrickslabs_testdatagenerator as datagen


class TestTopological_sort(unittest.TestCase):

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

        output = list(datagen.topological_sort(src))
        print("output1", output)

        self.assertEqual(output, ['id', 'code3a', '_r_code1', '_r_code3', 'code1', 'code2', 'code3'])

    def test_empty_list(self):
        src = []
        output = list(datagen.topological_sort(src))
        print("output", output)

        self.assertEqual(output, [])

    def test_singleton_list(self):
        src = [('id', [])]
        output = list(datagen.topological_sort(src))
        print("output", output)

        self.assertEqual(output, ['id'])
