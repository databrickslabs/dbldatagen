import pytest
import numpy as np
import pandas as pd
import re

from dbldatagen import TextGenerator, TemplateGenerator


# Test text and template generators without use of spark
class TestTextGeneratorBasic:
    testDataSpec = None
    row_count = 100000
    partitions_requested = 4

    @pytest.mark.parametrize("randomSeed", [None, 0, -1, 2112, 42])
    def test_text_generator_basic(self, randomSeed):
        text_gen1 = TextGenerator()
        text_gen2 = TextGenerator()

        if randomSeed is not None:
            text_gen1 = text_gen1.withRandomSeed(randomSeed)
            text_gen2 = text_gen1.withRandomSeed(randomSeed)

        assert repr(text_gen1) is not None
        assert str(text_gen1) is not None

        assert repr(text_gen2) is not None
        assert str(text_gen2) is not None

        assert text_gen1 == text_gen2

    @pytest.mark.parametrize("randomSeed, forceNewInstance", [(None, True), (None, False),
                                                              (0, True), (0, False),
                                                              (-1, True), (-1, False),
                                                              (2112, True), (2112, False),
                                                              (42, True), (42, False)])
    def test_text_generator_rng(self, randomSeed, forceNewInstance):
        text_gen1 = TextGenerator()
        text_gen2 = TextGenerator()

        if randomSeed is not None:
            text_gen1 = text_gen1.withRandomSeed(randomSeed)
            text_gen2 = text_gen1.withRandomSeed(randomSeed)

        rng1 = text_gen1.getNPRandomGenerator(forceNewInstance)
        rng2 = text_gen2.getNPRandomGenerator(forceNewInstance)

        rng1a = text_gen1.getNPRandomGenerator(forceNewInstance)

        # function should return cached instance if not forceNewInstance
        if not forceNewInstance:
            assert rng1 is rng1a

        # if seed is not None and seed is not -1, then both generators should generate same random sequence

        rng_shape = [1, 4]
        values1 = rng1.integers(0, 51, size=rng_shape, dtype=np.int32)
        values2 = rng2.integers(0, 51, size=rng_shape, dtype=np.int32)

        assert values1 is not None
        assert values2 is not None

        # sequence should be repeatable if there is a new instance
        if randomSeed is not None and randomSeed != -1 and forceNewInstance:
            assert (values1 == values2).all()

    @pytest.mark.parametrize("values, expectedType", [([1, 2, 3], np.uint8),
                                                      ([1, 40000, 3], np.uint16),
                                                      ([1, 40000.0, 3], np.uint16),
                                                      ([1, 40000.4, 3], np.uint16),
                                                      (np.array([1, 40000.4, 3]), np.uint16)
                                                      ])
    def test_text_generator_compact_types(self, values, expectedType):
        text_gen1 = TextGenerator()

        np_type = text_gen1.compactNumpyTypeForValues(values)
        assert np_type == expectedType

    @pytest.mark.parametrize("template, expectedOutput", [(r'53.123.ddd.ddd', r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"),
                                                          (r'\w.\W.\w.\W', r"[a-z]+\.[A-Z]+\.[a-z]+\.[A-Z]+"),
                                                          ])
    def test_templates_constant(self, template, expectedOutput):
        text_gen1 = TemplateGenerator(template)
        text_gen1 = text_gen1.withRandomSeed(2112)

        np_data = np.arange(100000)
        pd_data = pd.Series(np_data, copy=False)

        results = text_gen1.pandasGenerateText(pd_data)

        patt = re.compile(expectedOutput)
        for x in results[0:100]:
            assert patt.match(x), f"Expecting data '{x}'  to match pattern {patt}"

    @pytest.mark.parametrize("sourceData, template, expectedOutput", [(np.arange(1000),
                                                                       r'53.123.\V.\V', r"53\.123\.105\.105"),
                                                                      (np.arange(1000),
                                                                       r'\V.\V.\V.123', r"105\.105\.105\.123"),
                                                                      (np.arange(1000),
                                                                       r'\V.\W.\w.\W', r"105\.[A-Z]+\.[a-z]+\.[A-Z]+"),
                                                                      ([(x, x+1) for x in np.arange(1000)],
                                                                       r'\v0.\v1.\w.\W', r"105\.106\.[a-z]+\.[A-Z]+"),
                                                                      ])
    def test_template_value_substitution(self, sourceData, template, expectedOutput):
        """
        Test value substition for row 105

        :param template:
        :param expectedOutput:
        :return:
        """
        text_gen1 = TemplateGenerator(template)
        text_gen1 = text_gen1.withRandomSeed(2112)

        pd_data = pd.Series(sourceData, copy=False)

        results = text_gen1.pandasGenerateText(pd_data)

        patt = re.compile(expectedOutput)
        test_row = results[105]
        assert patt.match(test_row), f"Expecting data '{test_row}'  to match pattern {patt}"

    @pytest.mark.parametrize("template, expectedRandomNumbers", [(r'53.123.ddd.ddd', 6),
                                                                 (r'\w.\W.\w.\W', 4),
                                                                 (r'\w.\W.\w.\W|\w \w|\W \w \W', [4, 2, 3]),
                                                                 ])
    def test_prepare_templates(self, template, expectedRandomNumbers):
        text_gen1 = TemplateGenerator(template)
        text_gen1 = text_gen1.withRandomSeed(2112)

        i = 0
        for template in text_gen1.templates:
            if type(expectedRandomNumbers) is list:
                expectedVectorSize = expectedRandomNumbers[i]
            else:
                expectedVectorSize = expectedRandomNumbers
            i = i + 1

            vector_rnd = text_gen1.prepareStringsFromSingleTemplate(template)

            assert len(vector_rnd) == expectedVectorSize, f"template is '{template}'"

    def test_word_offset_generation(self):
        _WORDS_LOWER = ['lorem', 'ipsum', 'dolor', 'sit', 'amet', 'consectetur', 'adipiscing', 'elit', 'sed', 'do',
                        'eiusmod', 'tempor', 'incididunt', 'ut', 'labore', 'et', 'dolore', 'magna', 'aliqua', 'ut',
                        'enim', 'ad', 'minim', 'veniam', 'quis', 'nostrud', 'exercitation', 'ullamco', 'laboris',
                        'nisi', 'ut', 'aliquip', 'ex', 'ea', 'commodo', 'consequat', 'duis', 'aute', 'irure', 'dolor',
                        'in', 'reprehenderit', 'in', 'voluptate', 'velit', 'esse', 'cillum', 'dolore', 'eu', 'fugiat',
                        'nulla', 'pariatur', 'excepteur', 'sint', 'occaecat', 'cupidatat', 'non', 'proident', 'sunt',
                        'in', 'culpa', 'qui', 'officia', 'deserunt', 'mollit', 'anim', 'id', 'est', 'laborum']

        text_gen1 = TemplateGenerator(r"w.W.w.W")
        text_gen1 = text_gen1.withRandomSeed(2112)

        offsets = []

        for x in range(100000):
            offsets.append(text_gen1._getRandomWordOffset(len(_WORDS_LOWER)))

        min_val = np.min(offsets)
        max_val = np.max(offsets)
        assert min_val == 0
        assert max_val <= len(_WORDS_LOWER) - 1

        print(min_val, max_val, len(_WORDS_LOWER))
