from abc import ABC, abstractmethod
import logging
import pytest
from dbldatagen import optional_abstractmethod, is_optional_abstractmethod, \
    get_optional_abstract_methods, abstract_with_optional_methods

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestTypeUtils:

    @pytest.fixture()
    def marked_class(self):
        class MarkedClass(ABC):
            @optional_abstractmethod
            def method1(self):
                pass

            def method2(self):
                pass

        return MarkedClass

    def test_optional_abstractmethod(self, marked_class):
        assert is_optional_abstractmethod(marked_class.method1)
        assert not is_optional_abstractmethod(marked_class.method2)

        optional_methods = get_optional_abstract_methods(marked_class)
        assert len(optional_methods) == 1

    def test_bad_optional_abstractmethod(self):
        # this code should raise an error due to the method signature not matching the base class
        with pytest.raises(TypeError):
            @abstract_with_optional_methods
            class MarkedClass(ABC):
                @optional_abstractmethod
                def method1(self):
                    pass

                @abstractmethod
                def method2(self):
                    pass

            class MarkedClass2(MarkedClass):
                def method1(self, b):  # pylint: disable=arguments-differ
                    pass

    def test_bad_optional_abstractmethod2(self):
        # this code should raise an error due to the method signature being shadowed
        with pytest.raises(TypeError):
            @abstract_with_optional_methods
            class MarkedClass(ABC):
                @optional_abstractmethod
                def method1(self):
                    pass

                @abstractmethod
                def method2(self):
                    pass

            class MarkedClass2(MarkedClass):
                method1 = 52

    def test_good_optional_abstractmethod(self):
        # no need to explicitly assert as abc module automatically raises errors
        @abstract_with_optional_methods
        class MarkedClass(ABC):
            @optional_abstractmethod
            def method1(self):
                pass

            @abstractmethod
            def method2(self):
                pass

        class MarkedClass2(MarkedClass):
            def method1(self):
                pass

            def method2(self):
                pass

        x = MarkedClass2()
