from copy import deepcopy
import pytest

from pyspark.sql.types import StringType, TimestampType

import dbldatagen as dg


class TestColumnGenerationSpec:

    def test_deepcopy(self):
        cd = dg.ColumnGenerationSpec(name="test")
        results = deepcopy(cd)
        assert results.getNames() == cd.getNames()

    def test_tmp_name(self):
        cd = dg.ColumnGenerationSpec(name="test")

        with cd._temporaryRename("test2") as cd2:
            assert cd2.name == "test2"

        assert cd.name == "test"

    def test_tmp_name_exception(self):
        with pytest.raises(ValueError):

            cd = dg.ColumnGenerationSpec(name="test")

            with cd._temporaryRename("test2") as cd2:
                assert cd2.name == "test2"
                raise ValueError(1)

            assert cd.name == "test"

    def test_getNames(self):
        cd = dg.ColumnGenerationSpec(name="test")
        results = cd.getNames()
        assert results == ['test']

    def test_getNames2(self):
        cd = dg.ColumnGenerationSpec(name="test", numColumns=3)
        results = cd.getNames()
        assert results == ['test_0', 'test_1', 'test_2']

    def test_isFieldOmitted(self):
        cd = dg.ColumnGenerationSpec(name="test", omit=True)
        assert cd.omit, "Omit should be True"  # assert its true

    def test_colType(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType())
        assert cd.datatype == dt

        dt2 = TimestampType()
        cd2 = dg.ColumnGenerationSpec(name="test", colType=TimestampType())
        assert cd2.datatype == dt2

    def test_prefix(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), prefix="test_")
        assert cd.prefix == "test_"
        assert isinstance(cd.datatype, type(dt))

    def test_suffix(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), suffix="_test")
        assert cd.suffix == "_test"
        assert isinstance(cd.datatype, type(dt))

    def test_baseColumn(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), baseColumn='test0')
        assert cd.baseColumn == 'test0', "baseColumn should be as expected"
        assert cd.baseColumns == ['test0']
        assert isinstance(cd.datatype, type(dt))

    def test_baseColumnMultiple(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), baseColumn=['test0', 'test_1'])
        assert cd.baseColumn == ['test0', 'test_1'], "baseColumn should be as expected"
        assert cd.baseColumns == ['test0', 'test_1']
        assert isinstance(cd.datatype, type(dt))

    def test_baseColumnMultiple2(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), baseColumn='test0,test_1')
        assert cd.baseColumn == 'test0,test_1', "baseColumn should be as expected"
        assert cd.baseColumns == ['test0', 'test_1']
        assert isinstance(cd.datatype, type(dt))

    def test_expr(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), baseColumn='test0,test_1', expr="concat(1,2)")
        assert cd.expr == 'concat(1,2)'
        assert isinstance(cd.datatype, type(dt))

    def test_default_random_attribute(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), baseColumn='test0,test_1', expr="concat(1,2)")
        assert not cd.random, "random should be False by default"

    @pytest.mark.parametrize(
        "randomSetting, expectedSetting",
        [
            (True, True),
            (False, False),
        ],
    )
    def test_random_explicit(self, randomSetting, expectedSetting):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(
            name="test", colType=StringType(), baseColumn='test0,test_1', expr="concat(1,2)", random=randomSetting
        )

        assert cd.random is expectedSetting, f"random should be {expectedSetting}"
