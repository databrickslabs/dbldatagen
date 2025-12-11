# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the text generator plugin class `PyfuncText`
"""

import importlib
import logging
from collections.abc import Callable
from types import ModuleType
from typing import Optional

import pandas as pd

from dbldatagen.text_generators import TextGenerator
from dbldatagen.utils import DataGenError


class _FnCallContext:
    """
    Inner class for storing context between function calls.

    initial instances of random number generators, clients for services etc here during execution
    of the `initFn` calls

    :param txtGen: - reference to outer PyfnText object
    """

    textGenerator: "TextGenerator"

    def __init__(self, txtGen: "TextGenerator") -> None:
        self.textGenerator = txtGen

    def __setattr__(self, name: str, value: object) -> None:
        """Allow dynamic attribute setting for plugin context."""
        super().__setattr__(name, value)

    def __getattr__(self, name: str) -> object:
        """Allow dynamic attribute access for plugin context."""
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


class PyfuncText(TextGenerator):  # lgtm [py/missing-equals]
    """
    Text generator that supports generating text from an arbitrary Python function.

    :param fn: Python function which generates text
    :param init: Python function which creates an initial context/state
    :param initPerBatch: Whether to call the initialization function for each invocation of the Pandas UDF which
        generates text (default `false`)
    :param name: Optional name of the text generator when converted to string via ``repr`` or ``str``

    The two functions define the plugin model

    The first function, ``fn`` is called whenever text should be generated for a single column of a single row

    It is called with the signature ``fn(context, value)`` unless a root property is set, in which the signature is
    ``fn(rootProperty)`` with rootProperty having the value of the root property of the context.

    Context is the stored context containing instances of random number generators, 3rd party
    client library objects etc.

    The ``initFn`` is called to initialize the function call context. The plugin code can store arbitrary properties
    in the context following normal Python object rules.

    The context is initialized with the property `textGenerator` prior to being initialized which is a reference to the
    enclosing text generator.

    .. note::
      There are no expectations of repeatability of data generation when using external code
      or external libraries to generate text.

      However, custom code can call the base class method to get a Numpy random
      number generator instance. This will have been seeded using the ``dbldatagen``
      random number seed if one was specified, so random numbers generated from this will be repeatable.

      The custom code may call the property ``randomSeed`` on the text generator object to get the random seed
      which may be used to seed library specific initialization.

      This random seed property may have the values ``None`` or ``-1`` which should be treated as meaning dont
      use a random seed.

      The code does not guarantee thread or cross process safety. If a new instance of the random number
      generator is needed, you may call the base class method with the argument `forceNewInstance` set to True.
    """

    _name: str
    _initPerBatch: bool
    _rootProperty: object
    _pyFn: Callable
    _initFn: Callable | None
    _context: _FnCallContext | None

    def __init__(
        self,
        fn: Callable,
        *,
        init: Callable | None = None,
        initPerBatch: bool = False,
        name: str | None = None,
        rootProperty: object = None,
    ) -> None:
        super().__init__()
        if not callable(fn):
            raise ValueError("Function must be provided with signature fn(context, oldValue)")

        if init and not callable(init):
            raise ValueError("Init function must be a callable function or lambda if passed")

        # if root property is provided, root property will be passed to generate text function
        self._rootProperty = rootProperty
        self._pyFn = fn  # generate text function
        self._initFn = init  # context initialization function
        self._context = None  # context used to hold library root object and other properties

        # if init per batch is True, initialization of context will be per UDF call
        if not isinstance(initPerBatch, bool):
            raise ValueError("initPerBatch must evaluate to boolean True or False")

        self._initPerBatch = initPerBatch
        self._name = name if name is not None else "PyfuncText"

    def __str__(self) -> str:
        """
        Gets a string representation of the text generator using the ``name`` property.

        :returns: String representation of the text generator
        """
        return f"{self._name}({self._pyFn!r}, init={self._initFn})"

    def _getContext(self, forceNewInstance: bool = False) -> _FnCallContext:
        """
        Gets the context for plugin function calls.

        :param forceNewInstance: Whether to create a new context for each call (default `False`)
        :return: Existing or new context for plugin function calls
        """
        if self._context is None or forceNewInstance:
            context = _FnCallContext(self)

            # init context using context creator if any provided
            if self._initFn is not None:
                self._initFn(context)

            # save context for later use unless forceNewInstance is set
            if not forceNewInstance:
                self._context = context
            else:
                return context

        return self._context

    def pandasGenerateText(self, v: pd.Series) -> pd.Series:
        """
        Generates text from input columns using a Pandas UDF.

        :param v: Input column values as Pandas Series
        :returns: Generated text values as a Pandas Series or DataFrame
        """
        # save object properties in local vars to avoid overhead of object dereferences
        # on every call
        context = self._getContext(self._initPerBatch)
        evalFn = self._pyFn
        rootProperty = getattr(context, str(self._rootProperty), None) if self._rootProperty else None

        # define functions to call with context and with root property
        def _valueFromFn(originalValue: object) -> object:
            return evalFn(context, originalValue)

        def _valueFromFnWithRoot(_: object) -> object:
            return evalFn(rootProperty)

        if rootProperty is not None:
            return v.apply(_valueFromFnWithRoot)

        return v.apply(_valueFromFn)


class PyfuncTextFactory:
    """
    Applies syntactic wrapping around the creation of PyfuncText objects.

    :param name: Generated object name (when converted to string via ``str``)

    This class allows the use of the following constructs:

    .. code-block:: python

       # initialization (for Faker for example)

       # setup use of Faker
       def initFaker(ctx):
         ctx.faker = Faker(locale="en_US")
         ctx.faker.add_provider(internet)

       FakerText = (PyfuncTextFactory(name="FakerText")
                   .withInit(initFaker)        # determines how context should be initialized
                   .withRootProperty("faker")  # determines what context property is passed to fn
                   )

       # later use ...
       .withColumn("fake_name", text=FakerText("sentence", ext_word_list=my_word_list) )
       .withColumn("fake_sentence", text=FakerText("sentence", ext_word_list=my_word_list) )

       # translates to generation of lambda function with keyword arguments
       # or without as needed
       .withColumn("fake_name",
                   text=FakerText( (lambda faker: faker.name( )),
                                   init=initFaker,
                                   rootProperty="faker",
                                   name="FakerText"))
       .withColumn("fake_sentence",
                   text=FakerText( (lambda faker:
                                       faker.sentence( **{ "ext_word_list" : my_word_list} )),
                                   init=initFaker,
                                   rootProperty="faker",
                                   name="FakerText"))
    """
    _name: str
    _initPerBatch: bool
    _initFn: Callable | None
    _rootProperty: object | None

    _name: str
    _initPerBatch: bool
    _initFn: Callable | None
    _rootProperty: object | None

    def __init__(self, name: str | None = None) -> None:
        self._initFn = None
        self._rootProperty = None
        self._name = "PyfuncText" if name is None else name
        self._initPerBatch = False

    def withInit(self, fn: Callable) -> "PyfuncTextFactory":
        """
        Sets the initialization function for creating context.

        :param fn: Callable function for initializing context; Signature should ``initFunction(context)``
        :returns: Modified text generation factory with the specified initialization function

        .. note::
           This variation initializes the context once per worker process per text generator
           instance.
        """
        self._initFn = fn
        return self

    def withInitPerBatch(self, fn: Callable) -> "PyfuncTextFactory":
        """
        Sets the initialization function for creating context for each batch.

        :param fn: Callable function for initializing context; Signature should ``initFunction(context)``
        :returns: Modified text generation factory with the specified initialization function called for each batch

        .. note::
           This variation initializes the context once per internal pandas UDF call.
           The UDF call will be called once per 10,000 rows if system is configured using defaults.
           Setting the pandas batch size as an argument to the DataSpec creation will change the default
           batch size.
        """
        self._initPerBatch = True
        return self.withInit(fn)

    def withRootProperty(self, prop: object) -> "PyfuncTextFactory":
        """
        Sets the context property to be passed to the text generation function. If not called, the context object will
        be passed to the text generation function.

        :param prop: Context property
        :returns: Modified text generation factory with the context property
        """
        self._rootProperty = prop
        return self

    def __call__(self, evalFn: str | Callable, *args, isProperty: bool = False, **kwargs) -> PyfuncText:
        """
        Internal function calling mechanism that implements the syntax expansion.

        :param evalFn: Callable text generation function
        :param args: Optional arguments to pass by position to the text generation function
        :param kwargs: Optional keyword arguments following Python keyword passing mechanism
        :param isProperty: Whether to interpret the evaluation function as string name of property instead of a callable
            function (default `False`)
        """
        assert evalFn is not None and (type(evalFn) is str or callable(evalFn)), "Function must be provided"

        if isinstance(evalFn, str):
            if not self._rootProperty:
                raise ValueError("String named functions can only be used on text generators with root property")
            function_name = evalFn

            if (len(args) > 0 or len(kwargs) > 0) and isProperty:
                raise ValueError("Argument 'isProperty' cannot be used when passing arguments")

            def generated_evalFn(root: object) -> object:
                method = getattr(root, function_name)

                if isProperty:
                    return method
                elif len(args) > 0 and len(kwargs) > 0:
                    return method(*args, **kwargs)
                elif len(args) > 0:
                    return method(*args)
                elif len(kwargs) > 0:
                    return method(**kwargs)
                else:
                    return method()

            evalFn = generated_evalFn

        # returns the actual PyfuncText text generator object.
        # Note all syntax expansion is performed once only
        return PyfuncText(evalFn, init=self._initFn, name=self._name, rootProperty=self._rootProperty)


class FakerTextFactory(PyfuncTextFactory):
    """
    Factory for creating Faker text generators.

    :param locale: Optional list of locales (default is ``["en-US"]``)
    :param providers: List of providers
    :param name: Optional name of generated objects (default is ``FakerText``)
    :param lib: Optional import alias of Faker library (dfault is ``"faker"``)
    :param rootClass: Optional name of the root object class (default is ``"Faker"``)

    ..note ::
       Both the library name and root object class can be overridden - this is primarily for internal testing purposes.
    """

    _defaultFakerTextFactory: Optional["FakerTextFactory"] = None
    _FAKER_LIB: str = "faker"

    def __init__(
        self,
        *,
        locale: str | list[str] | None = None,
        providers: list | None = None,
        name: str = "FakerText",
        lib: str | None = None,
        rootClass: str | None = None,
    ) -> None:

        super().__init__(name)

        # set up the logger
        self._logger = logging.getLogger("FakerTextFactory")
        self._logger.setLevel(logging.WARNING)

        # setup Faker library to use
        if lib is None:
            lib = self._FAKER_LIB

        # allow overriding the root object class for test purposes
        if rootClass is None:
            self._rootObjectClass = "Faker"
        else:
            self._rootObjectClass = rootClass

        # load the library
        faker_module = self._loadLibrary(lib)

        # make the initialization function
        init_function = self._mkInitFn(faker_module, locale, providers)

        self.withInit(init_function)
        self.withRootProperty("faker")

    @classmethod
    def _getDefaultFactory(cls, lib: str | None = None, rootClass: str | None = None) -> "FakerTextFactory":
        """
        Gets a default faker text factory.

        :param lib: Optional import alias of Faker library (dfault is ``"faker"``)
        :param rootClass: Optional name of the root object class (default is ``"Faker"``)
        """
        if cls._defaultFakerTextFactory is None:
            cls._defaultFakerTextFactory = FakerTextFactory(lib=lib, rootClass=rootClass)
        return cls._defaultFakerTextFactory

    def _mkInitFn(self, libModule: object, locale: str | list[str] | None, providers: list | None) -> Callable:
        """
        Creates a Faker initialization function.

        :param libModule: Faker module
        :param locale: Locale string or list of locale strings (e.g. "en-us")
        :param providers: List of Faker providers to load
        :returns: Callable initialization function
        """
        if libModule is None:
            raise ValueError("must have a valid loaded Faker library module")

        fakerClass = getattr(libModule, self._rootObjectClass)

        # define the initialization function for Faker
        def fakerInitFn(ctx: _FnCallContext) -> None:
            if locale is not None:
                ctx.faker = fakerClass(locale=locale)
            else:
                ctx.faker = fakerClass()

            if providers is not None:
                for provider in providers:
                    ctx.faker.add_provider(provider)  # type: ignore[attr-defined]

        return fakerInitFn

    def _loadLibrary(self, lib: str) -> ModuleType:
        """
        Loads the faker library.

        :param lib: Optional alias name for Faker library (default is ``"faker"``)
        """
        try:
            if lib is not None:
                if not isinstance(lib, str):
                    raise ValueError(f"Input Faker alias with type '{type(lib)}' must be of type 'str'")

                if not lib:
                    raise ValueError("Input Faker alias must be provided")

                if lib in globals():
                    module = globals()[lib]
                    if isinstance(module, ModuleType):
                        return module
                    else:
                        raise ValueError(f"Global '{lib}' is not a module")

                else:
                    fakerModule = importlib.import_module(lib)
                    globals()[lib] = fakerModule
                    return fakerModule
            else:
                raise ValueError("Library name must be provided")

        except RuntimeError as err:
            raise DataGenError("Could not load or initialize Faker library") from err


def fakerText(mname: str, *args, _lib: str | None = None, _rootClass: str | None = None, **kwargs) -> PyfuncText:
    """
    Creates a faker text generator object using the default ``FakerTextFactory`` instance. Calling this method is
    equivalent to calling ``FakerTextFactory()("sentence")``.

       :param mname: Method name to invoke
       :param args: Positional argumentss to pass to the Faker text generation method
       :param _lib: Optional import alias of Faker library (default is ``"faker"``)
       :param _rootClass: Optional name of the root object class (default is ``"Faker"``)
       :returns : ``PyfuncText`` for use with Faker
    """
    default_factory = FakerTextFactory._getDefaultFactory(lib=_lib, rootClass=_rootClass)
    return default_factory(mname, *args, **kwargs)  # pylint: disable=not-callable
