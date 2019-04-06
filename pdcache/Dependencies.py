# -*- coding:utf-8 -*-

from pdcache import get_logger, Logger


class Dependencies:
    def __init__(self, recursive: bool = False, logger: Logger = None):
        """
        Dependencies Stack Dict
        {
            column_name1(str): [column_name1_1(str), column_name1_2(str), ...],
            column_name2(str): [column_name2_1(str), column_name2_2(str), ...]
        }

        about recursive flag,
        e.g. recursive is False:
            dependencies
                {col_0: [col_0_0, col_0_1]}
                {col_1: [col_1_0, col_0]}

        e.g. recursive is True:
            dependencies
                {col_0: [col_0_0, col_0_1]}
                {col_1: [col_1_0, col_0_0, col_0_1]}

        :param recursive:
        :param logger:
        """
        self._recursive = recursive
        self._logger = logger or get_logger(__name__)
        self._dependencies_dict = {}

    def __getitem__(self, item):
        try:
            return self._dependencies_dict[item]
        except KeyError as err_msg:
            self._logger.error(err_msg)
            return None

    def __setitem__(self, key, value):
        self.assign_dependencies(key, value)

    @property
    def columns(self):
        return self._dependencies_dict.keys()

    @property
    def as_dict(self):
        return self._dependencies_dict

    def _recursive_dependencies(self, dependency_list: list) -> list:
        """
        :param dependency_list: list of str
        :return:
        """
        column_names = self.columns
        dependencies = []
        for column_name in dependency_list:
            if column_name not in column_names:
                self.assign_dependencies(column_name, [])
            dependencies.append(column_name)
            dependencies += self._dependencies_dict[column_name]
        return list(set(dependencies))

    def assign_dependencies(self, column_name: str, dependency_list: list):
        """
        :param column_name:
        :param dependency_list: list of str
        """
        if self._recursive:
            dependency_list = self._recursive_dependencies(dependency_list)
        self._dependencies_dict[column_name] = dependency_list
