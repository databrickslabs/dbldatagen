from unittest.mock import patch

from dbldatagen.spark_compat import is_spark_connect_session


class FakeConnectSparkSession:
    pass


def test_identifies_connect_spark_session():
    with patch("dbldatagen.spark_compat.ConnectSparkSession", FakeConnectSparkSession):
        assert is_spark_connect_session(FakeConnectSparkSession())


def test_rejects_non_connect_spark_session():
    with patch("dbldatagen.spark_compat.ConnectSparkSession", FakeConnectSparkSession):
        assert not is_spark_connect_session(object())
        assert not is_spark_connect_session()
