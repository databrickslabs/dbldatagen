import pytest
import numpy as np

from dbldatagen import TextGenerator

# Test manipulation and generation of test data for a large schema
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

    @pytest.mark.parametrize("randomSeed, forceNewInstance", [(None,True), (None, False),
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










