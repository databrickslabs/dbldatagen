# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the text generator plugin class `PyfuncText`
"""

import importlib
import logging

from .text_generators import TextGenerator
from .utils import DataGenError


class PyfuncText(TextGenerator):  # lgtm [py/missing-equals]
    """ Text generator that supports generating text from arbitrary Python function

    :param fn: function to call to generate text.
    :param init: function to call to initialize context
    :param initPerBatch: if init per batch is set to True, initialization of context is performed on every Pandas udf
                         call. Default is False.
    :param name: String representing name of text generator when converted to string via ``repr`` or ``str``

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

    class _FnCallContext:
        """ inner class to support storage of context between calls

            initial instances of random number generators, clients for services etc here during execution
            of the `initFn` calls

            :param txtGen: - reference to outer PyfnText object

        """

        def __init__(self, txtGen):
            self.textGenerator = txtGen

    def __init__(self, fn, init=None, initPerBatch=False, name=None, rootProperty=None):
        super().__init__()
        assert fn is not None or callable(fn), "Function must be provided wiith signature fn(context, oldValue)"
        assert init is None or callable(init), "Init function must be a callable function or lambda if passed"

        # if root property is provided, root property will be passed to generate text function
        self._rootProperty = rootProperty

        self._pyFn = fn  # generate text function
        self._initFn = init  # context initialization function
        self._context = None  # context used to hold library root object and other properties

        # if init per batch is True, initialization of context will be per UDF call
        assert initPerBatch in [True, False], "initPerBatch must evaluate to boolean True or False"
        self._initPerBatch = initPerBatch

        self._name = name if name is not None else "PyfuncText"

    def __str__(self):
        """ Get string representation of object
            ``name`` property is used to provide user friendly name for text generator
        """
        return f"{self._name}({repr(self._pyFn)}, init={self._initFn})"

    def _getContext(self, forceNewInstance=False):
        """ Get the context for plugin function calls

        :param forceNewInstance: if True, forces each call to create a new context
        :return: existing or newly created context.

        """
        context = self._context
        if context is None or forceNewInstance:
            context = PyfuncText._FnCallContext(self)

            # init context using context creator if any provided
            if self._initFn is not None:
                self._initFn(context)

            # save context for later use unless forceNewInstance is set
            if not forceNewInstance:
                self._context = context
            else:
                return context
        return self._context

    def pandasGenerateText(self, v):
        """ Called to generate text via Pandas UDF mechanism

        :param v: base value of column as Pandas Series

        """
        # save object properties in local vars to avoid overhead of object dereferences
        # on every call
        context = self._getContext(self._initPerBatch)
        evalFn = self._pyFn
        rootProperty = getattr(context, self._rootProperty) if self._rootProperty is not None else None

        # define functions to call with context and with root property
        def _valueFromFn(originalValue):
            return evalFn(context, originalValue)

        def _valueFromFnWithRoot(originalValue):
            return evalFn(rootProperty)

        if rootProperty is not None:
            results = v.apply(_valueFromFnWithRoot, args=None)
        else:
            results = v.apply(_valueFromFn, args=None)

        return results


class PyfuncTextFactory:
    """PyfuncTextFactory applies syntactic wrapping around creation of PyfuncText objects

    :param name: name of generated object (when converted to string via ``str``)

    It allows the use of the following constructs:

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

    def __init__(self, name=None):
        """

        :param name: name of generated object (when converted to string via ``str``)

        """
        self._initFn = None
        self._rootProperty = None
        self._name = "PyfuncText" if name is None else name
        self._initPerBatch = False

    def withInit(self, fn):
        """ Specifies context initialization function

            :param fn: function pointer or lambda function for initialization
                       signature should ``initFunction(context)``

            .. note::
               This variation initializes the context once per worker process per text generator
               instance.
        """
        self._initFn = fn
        return self

    def withInitPerBatch(self, fn):
        """ Specifies context initialization function

            :param fn: function pointer or lambda function for initialization
                       signature should ``initFunction(context)``

            .. note::
               This variation initializes the context once per internal pandas UDF call.
               The UDF call will be called once per 10,000 rows if system is configured using defaults.
               Setting the pandas batch size as an argument to the DataSpec creation will change the default
               batch size.
        """
        self._initPerBatch = True
        return self.withInit(fn)

    def withRootProperty(self, prop):
        """ If called, specifies the property of the context to be passed to the text generation function.
            If not called, the context object itself will be passed to the text generation function.
        """
        self._rootProperty = prop
        return self

    def __call__(self, evalFn, *args, isProperty=False, **kwargs):
        """ Internal function call mechanism that implements the syntax expansion

        :param evalFn: text generation function or lambda
        :param args: optional args to be passed by position
        :param kwargs: optional keyword args following Python keyword passing mechanism
        :param isProperty: if true, interpret evalFn as string name of property, not a function or method
        """
        assert evalFn is not None and (type(evalFn) is str or callable(evalFn)), "Function must be provided"

        if type(evalFn) is str:
            assert self._rootProperty is not None and len(self._rootProperty.strip()) > 0, \
                "string named functions can only be used on text generators with root property"
            fnName = evalFn
            if len(args) > 0 and len(kwargs) > 0:
                # generate lambda with both kwargs and args
                assert not isProperty, "isProperty cannot be true if using arguments"
                evalFn = lambda root: getattr(root, fnName)(*args, **kwargs)
            elif len(args) > 0:
                # generate lambda with positional args
                assert not isProperty, "isProperty cannot be true if using arguments"
                evalFn = lambda root: getattr(root, fnName)(*args)
            elif len(kwargs) > 0:
                # generate lambda with keyword args
                assert not isProperty, "isProperty cannot be true if using arguments"
                evalFn = lambda root: getattr(root, fnName)(**kwargs)
            elif isProperty:
                # generate lambda with property access, not method call
                evalFn = lambda root: getattr(root, fnName)
            else:
                # generate lambda with no args
                evalFn = (lambda root: getattr(root, fnName)())

        # returns the actual PyfuncText text generator object.
        # Note all syntax expansion is performed once only
        return PyfuncText(evalFn, init=self._initFn, name=self._name, rootProperty=self._rootProperty)


class FakerTextFactory(PyfuncTextFactory):
    """ Factory object for Faker text generator flavored ``PyfuncText`` objects

    :param locale: list of locales. If empty, defaults to ``en-US``
    :param providers: list of providers
    :param name: name of generated objects. Defaults to ``FakerText``
    :param lib: library import name of Faker library. If none passed, uses ``faker``
    :param rootClass: name of root object class If none passed, uses ``Faker``

    ..note ::
       Both the library name and root object class can be overridden - this is primarily for internal testing purposes.
    """

    _FAKER_LIB = "faker"

    _defaultFakerTextFactory = None

    def __init__(self, locale=None, providers=None, name="FakerText", lib=None,
                 rootClass=None):

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
        fakerModule = self._loadLibrary(lib)

        # make the initialization function
        initFn = self._mkInitFn(fakerModule, locale, providers)

        self.withInit(initFn)
        self.withRootProperty("faker")

    @classmethod
    def _getDefaultFactory(cls, lib=None, rootClass=None):
        """Class method to get default faker text factory

           Not intended for general use
        """
        if cls._defaultFakerTextFactory is None:
            cls._defaultFakerTextFactory = FakerTextFactory(lib=lib, rootClass=rootClass)
        return cls._defaultFakerTextFactory

    def _mkInitFn(self, libModule, locale, providers):
        """ Make Faker initialization function

        :param locale: locale string or list of locale strings
        :param providers: providers to load
        :return:
        """
        assert libModule is not None, "must have a valid loaded Faker library module"

        fakerClass = getattr(libModule, self._rootObjectClass)

        # define the initialization function for Faker
        def fakerInitFn(ctx):
            if locale is not None:
                ctx.faker = fakerClass(locale=locale)
            else:
                ctx.faker = fakerClass()

            if providers is not None:
                for provider in providers:
                    ctx.faker.add_provider(provider)

        return fakerInitFn

    def _loadLibrary(self, lib):
        """ Load faker library if not already loaded

        :param lib: library name of Faker library. If none passed, uses ``faker``
        """
        # load library
        try:
            if lib is not None:
                assert type(lib) is str and len(lib.strip()), f"Library ``{lib}`` must be a valid library name"

                if lib in globals():
                    return globals()[lib]
                else:
                    fakerModule = importlib.import_module(lib)
                    globals()[lib] = fakerModule
                    return fakerModule
        except RuntimeError as err:
            # pylint: disable=raise-missing-from
            raise DataGenError("Could not load or initialize Faker library", err)


def fakerText(mname, *args, _lib=None, _rootClass=None, **kwargs):
    """Generate faker text generator object using default FakerTextFactory
       instance

       :param mname: method name to invoke
       :param args: positional args to be passed to underlying Faker instance
       :param _lib: internal only param - library to load
       :param _rootClass: internal only param - root class to create
       
       :returns : instance of PyfuncText for use with Faker

       ``fakerText("sentence")`` is same as ``FakerTextFactory()("sentence")``
    """
    defaultFactory = FakerTextFactory._getDefaultFactory(lib=_lib,
                                                         rootClass=_rootClass)
    return defaultFactory(mname, *args, **kwargs)  # pylint: disable=not-callable
