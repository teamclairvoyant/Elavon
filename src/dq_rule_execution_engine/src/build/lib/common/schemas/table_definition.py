from abc import ABC, abstractmethod


class TableDefinition(ABC):

    def __init__(
        self,
        table_name,
        dataset_name=None,
    ):
        self._DATASET_NAME = dataset_name
        self._TABLE_NAME = table_name

    def get_qualified_table_name(self):
        if self._DATASET_NAME is None:
            return self._TABLE_NAME
        else:
            return '.'.join([self._DATASET_NAME,  self._TABLE_NAME])

    @abstractmethod
    def get_schema(self):
        None
