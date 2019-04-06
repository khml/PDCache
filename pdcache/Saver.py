# -*- coding:utf-8 -*-

import os
import pickle
import datetime

import pandas as pd

from pdcache import get_logger, Logger


def save_as_pkl(path_to_pkl: str, data: object) -> None:
    """
    :param path_to_pkl: save path
    :param data:
    """
    with open(path_to_pkl, "wb") as f:
        pickle.dump(data, f)


def load_from_pkl(path_to_pkl: str) -> object:
    """
    :param path_to_pkl: pkl path
    :return: data from pkl
    """
    with open(path_to_pkl, "rb") as f:
        return pickle.load(f)


def get_timestamp_as_datetime(path_to_file):
    return datetime.date.fromtimestamp(os.path.getmtime(path_to_file))


class BaseSaver:
    def __init__(self, path_to_cache_dir: str, logger: Logger = None):
        """
        Cache Class
        save data as pkl, and load data from pkl.

        :param path_to_cache_dir:
        :param logger:
        """
        self._logger = logger or get_logger(__name__)
        self.cache_folder = path_to_cache_dir

    def _path_to_cache(self, name: str) -> str:
        return os.path.join(self.cache_folder, name)

    def is_exist(self, name: str) -> bool:
        path_to_cache = self._path_to_cache(name)
        return os.path.exists(path_to_cache)

    def timestamp(self, name: str) -> datetime.date:
        path_to_cache = self._path_to_cache(name)
        return get_timestamp_as_datetime(path_to_cache)

    @property
    def logger(self):
        return self._logger


class PickleSaver(BaseSaver):
    def save(self, data: object, feature: str) -> None:
        """
        save data as pkl
        save name is path/to/cache/dir/feature.pkl
        :param data:
        :param feature:
        """
        cache_path = self._path_to_cache(feature)
        self.logger.info("save data to {}".format(cache_path))
        save_as_pkl(cache_path, data)

    def load(self, feature: str, raise_error_when_not_exist: bool = False) -> object:
        """
        load data from pkl
        pkl name is path/to/cache/dir/feature.pkl
        :param feature:
        :param raise_error_when_not_exist:
        :return:
        """
        cache_path = self._path_to_cache(feature)
        if not os.path.exists(cache_path):
            self.logger.error("not find cache : {}".format(cache_path))
            if raise_error_when_not_exist:
                raise KeyError
            else:
                return None

        self.logger.info("load data from {}".format(cache_path))
        data = load_from_pkl(cache_path)
        return data


class PandasCache(BaseSaver):
    def __init__(self, path_to_cache_dir: str, logger: Logger = None):
        """
        make pandas DataFrame Cache
        :param path_to_cache_dir: default dir is .cache
        :param logger: default is Python Debug Level Logger
        """
        super().__init__(path_to_cache_dir, logger)
        self._pickle_cache = PickleSaver(path_to_cache_dir=path_to_cache_dir, logger=self.logger)

    def save(self, df: pd.DataFrame, column_names: list = None):
        """
        :param df:
        :param column_names: list of str. default is None, it means save all data.
        """
        column_names = column_names or df.columns.values.tolist()

        self.logger.info("cache data...")
        for column_name in column_names:
            self.logger.info("cache data : {}".format(column_name))
            data = df[column_name]
            self._pickle_cache.save(data, column_name)

    def load(self, column_names: list, df: pd.DataFrame = None,
             raise_error_when_not_exist: bool = False) -> pd.DataFrame:
        """
        :param column_names: list of str
        :param df: default is None, it means return new DataFrame obj. when set DataFrame obj, assign data to it.
        :param raise_error_when_not_exist:
        :return: data from cache
        """
        df = df or pd.DataFrame()

        self.logger.info("load data from cache...")
        for column_name in column_names:
            self.logger.info("load data : {}".format(column_name))
            cache = self._pickle_cache.load(column_name, raise_error_when_not_exist)
            df[column_name] = cache

        return df
