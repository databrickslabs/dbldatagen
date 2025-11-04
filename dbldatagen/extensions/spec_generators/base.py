from abc import ABC, abstractmethod
from dbldatagen.extensions.datagen_spec import DatagenSpec


class AbstractSpecGenerator(ABC):
    @abstractmethod
    def generate_spec(self) -> DatagenSpec:
        """
        Generates the DatagenSpec from the specific source.
        Should raise an exception (e.g., SpecGenerationError) if generation fails.
        """
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__}>"
