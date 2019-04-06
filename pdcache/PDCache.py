# -*- coding:utf-8 -*-

import datetime

from pdcache import get_logger, Logger, PandasCache, Dependencies


class PDCache:
    def __init__(self,
                 dependencies: Dependencies = None,
                 path_to_cache_dir: str = ".cache",
                 base_time: datetime.date = None,
                 logger: Logger = None):
        self._logger = logger or get_logger(__name__)
        self._cache = PandasCache(path_to_cache_dir, logger=logger)
        self._time_format = "%Y%m%d_%H%M%S"
        self._base_time = base_time or datetime.datetime.now()
        self.dependencies = dependencies or Dependencies(logger=self._logger)

        self._logger.debug("base time is {}".format(self._base_time.strftime(self._time_format)))

    def _is_cache_latest(self, column_name: str, base_time: datetime.date) -> bool:
        """
        check cache is latest or old.
        if all dependencies cache are older than base_time, the column cache is latest.
        :param column_name:
        :param base_time:
        :return
        """
        dependencies: list = self.dependencies[column_name]
        for name in dependencies:
            cache_timestamp = self._cache.timestamp(name)
            if cache_timestamp is None:
                return False
            if cache_timestamp > base_time:
                return False
        return True

    def save(self, df, column_names=None):
        self._cache.save(df, column_names)

    def load_data(self, column_name, base_time=None):
        base_time = base_time or self._base_time
        if not self._cache.is_exist(column_name):
            self._logger.warning("No Cache for {}".format(column_name))
            return None

        if not self._is_cache_latest(column_name, base_time):
            self._logger.info("It's Too Old Cache for {}:".format(column_name))
            return False

        cache = self._cache.load(column_name)
        return cache
