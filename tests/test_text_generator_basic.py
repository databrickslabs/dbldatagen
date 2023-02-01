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

    @pytest.mark.parametrize("sourceData, template, expectedOutput", [(np.arange(100000),
                                                                       r'53.123.\V.\V', r"53\.123\.105\.105"),
                                                                      (np.arange(100000),
                                                                       r'\V.\V.\V.123', r"105\.105\.105\.123"),
                                                                      (np.arange(1000),
                                                                       r'\V.\W.\w.\W', r"105\.[A-Z]+\.[a-z]+\.[A-Z]+"),
                                                                      ([[x, x + 1] for x in np.arange(10000)],
                                                                       r'\v0.\v1.\w.\W', r"105\.106\.[a-z]+\.[A-Z]+"),
                                                                      ([(x, x + 1) for x in range(10000)],
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

        np_data = np.array(sourceData, dtype=np.object_)
        pd_data = pd.Series(list(np_data), copy=False)


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

            placeholders, vector_rnd = text_gen1._prepareTemplateStrings(template)

            assert len(vector_rnd) == expectedVectorSize, f"template is '{template}'"
            assert placeholders > len(vector_rnd)

    @pytest.mark.parametrize("template, expectedRandomNumbers", [(r'53.123.ddd.ddd', 6),
                                                                 (r'\w.\W.\w.\W', 4),
                                                                 (r'\w.\W.\w.\W|\w \w|\W \w \W', [4, 2, 3]),
                                                                 ])
    def test_prepare_bounds(self, template, expectedRandomNumbers):
        text_gen1 = TemplateGenerator(template)
        text_gen1 = text_gen1.withRandomSeed(2112)

        data = np.arange(30000)
        pd_data = pd.Series(data)

        template_choices, template_rnd_bounds, template_rnds = text_gen1._prepare_random_bounds(pd_data)

        min_template, max_template = np.min(template_choices), np.max(template_choices)

        assert min_template >= 0
        assert max_template <= len(text_gen1.templates)

        assert template_choices.shape[0] == template_rnd_bounds.shape[0]
        assert template_choices.shape[0] == template_rnds.shape[0]
        assert template_choices.shape[0] == data.shape[0]

        # check that random values actually have random values
        for x in pd.Series(list(template_rnds)):
            distinct_values = set(x)
            assert (len(x) == 1) or (len(distinct_values) > 0)

        print(template_rnds)

    @pytest.mark.parametrize("template, expectedOutput", [(r'53.123.ddd.ddd', r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"),
                                                          (r'\w.\W.\w.\W', r"[a-z]+\.[A-Z]+\.[a-z]+\.[A-Z]+"),
                                                          (r'\w.\W.\w.\W|\w \w', r"[a-zA-Z\. ]+"),
                                                          (r'Dddd', r"[1-9][0-9]+"),
                                                          (r'\xxxxx \xXXX', r"x[0-9a-f]+ x[0-9A-F]+"),
                                                          (r'Aaaa Kkkk \N \n', r"[a-zA-Z]+ [0-9A-Za-z]+ [0-9]+ [0-9]+"),
                                                          (r'Aaaa \V \N \n', r"[A-Za-z]+ [0-9]+ [0-9]+ [0-9]+"),
                                                          ])
    def test_apply_template(self, template, expectedOutput):
        """ This method tests the core logic of the template generator
        """
        text_gen1 = TemplateGenerator(template)
        text_gen1 = text_gen1.withRandomSeed(2112)

        # generate test data
        data = np.arange(100000)
        pd_data = pd.Series(data)

        # prepare template selections, bounds, rnd values to drive application of algorithm
        template_choices, template_rnd_bounds, template_rnds = text_gen1._prepare_random_bounds(pd_data)
        template_choices_t = template_choices.T

        # save copies to ensure lookup values not modified by process
        template_rnds2 = template_rnds.copy()
        template_choices2 = template_choices.copy()
        data2 = data.copy()

        # placeholders is numpy array used to hold results
        placeholders = np.full((data.shape[0], text_gen1._max_placeholders), "", dtype=np.object_)

        # create masked arrays, with all elements initially masked
        # as we substitute template expansion, we'll mask and unmask rows corresponding to each template
        # calling the method to substitute the values on the masked placeholders
        masked_placeholders = np.ma.MaskedArray(placeholders, mask=False)
        masked_rnds = np.ma.MaskedArray(template_rnds, mask=False)
        masked_matrices = [masked_placeholders, masked_rnds]

        # test logic for template expansion
        for x in range(len(text_gen1._templates)):
            masked_placeholders[template_choices_t != x, :] = np.ma.masked
            masked_rnds[template_choices_t != x, :] = np.ma.masked

            # harden mask, preventing modifications
            for m in masked_matrices:
                np.ma.harden_mask(m)

            # expand values into placeholders
            text_gen1._applyTemplateStringsForTemplate(pd_data,
                                                       text_gen1._templates[x],
                                                       masked_placeholders,
                                                       masked_rnds
                                                       )

            # soften mask, allowing modifications
            for m in masked_matrices:
                np.ma.soften_mask(m)
                m.mask = False

        # join strings in placeholders
        output = pd.Series(list(placeholders))
        results = output.apply(lambda placeholder_items: "".join([str(elem) for elem in placeholder_items]))

        # check that process does not modify data
        assert (template_rnds2 == template_rnds).all()
        assert (template_choices2 == template_choices).all()
        assert (data2 == data).all()

        match_patt = re.compile(expectedOutput)
        for r in results:
            assert match_patt.match(r), f"expected '{r}' to match pattern '{expectedOutput}'"
