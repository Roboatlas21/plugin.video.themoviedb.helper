#!/usr/bin/python
# -*- coding: utf-8 -*-
from jurialmunkey.ftools import cached_property
from contextlib import contextmanager
from tmdbhelper.lib.addon.logger import kodi_log


class DatabaseConnection:
    def __init__(self, cache):
        self.cache = cache
        self.open_connection = None
        self._pending_close = None

    @staticmethod
    def _log_cleanup_error(error):
        try:
            kodi_log(f'CACHE: ERROR closing shared _database: {error}', 2)
        except Exception:
            # Do not replace the actual query/caller exception with a log error.
            pass

    def _close_pending(self):
        database = self._pending_close
        if database is None:
            return
        database.close()
        self._pending_close = None

    def close(self, suppress_db_close_error=False):
        cursor = self.open_connection
        if cursor is not None:
            # Detach the cursor; retain the database until cleanup succeeds.
            database = cursor.connection
            self.open_connection = None
            self._pending_close = database
            try:
                cursor.close()
            except BaseException:
                try:
                    self._close_pending()
                except BaseException as close_error:
                    self._log_cleanup_error(close_error)
                raise
        try:
            self._close_pending()
        except Exception as error:
            if not suppress_db_close_error:
                raise
            self._log_cleanup_error(error)

    @contextmanager
    def open(self):
        if self.open_connection is not None:
            yield self.open_connection
            return

        # Only the outer scope owns cleanup. Never reuse a failed-close handle.
        self._close_pending()
        database = self.cache.get_database()
        if database is None:
            raise RuntimeError('Database connection unavailable')
        self._pending_close = database
        try:
            self.open_connection = database.cursor()
        except BaseException:
            try:
                self._close_pending()
            except BaseException as error:
                self._log_cleanup_error(error)
            raise
        self._pending_close = None
        try:
            yield self.open_connection
        except BaseException:
            try:
                self.close()
            except BaseException as error:
                self._log_cleanup_error(error)
            raise
        else:
            self.close(suppress_db_close_error=True)


class DatabaseAccess:

    @cached_property
    def connection(self):
        return DatabaseConnection(self.cache)

    @property
    def open_connection(self):
        return self.connection.open_connection

    # ======================================
    # Single item with single key/value pair
    # ======================================

    def del_cached(self, table, item_id, children=True):
        self.cache.del_item(table, item_id, connection=self.open_connection)
        if not children:
            return
        self.cache.del_item_like(table, f'{item_id}.%', connection=self.open_connection)

    def get_cached(self, table, item_id, key):
        data = self.cache.get_values(table, item_id, keys=(key, ), connection=self.open_connection)
        return data[0] if data else None

    def set_cached(self, table, item_id, key, value):
        """ Set key to value for id=item_id in table """
        self.cache.set_item_values(table, item_id, keys=(key, ), values=(value, ), connection=self.open_connection)

    def use_cached(self, table, item_id, key, func, *args, **kwargs):
        """ Get key for id=item_id in table else set key to func(*args, **kwargs) """
        internal_data = self.get_cached(table, item_id, key)

        def get_external_data():
            external_data = func(*args, **kwargs)
            if not external_data:
                return
            self.set_cached(table, item_id, key, external_data)
            return self.get_cached(table, item_id, key)

        return internal_data or get_external_data()

    # =========================================
    # Single item with multiple key/value pairs
    # =========================================

    def get_cached_values(self, table, item_id, keys):
        return self.cache.get_values(table, item_id, keys, connection=self.open_connection)

    def set_cached_values(self, table, item_id, keys, values):
        self.cache.set_item_values(table, item_id, keys, values, connection=self.open_connection)

    # ============================================
    # Multiple items with multiple key/value pairs
    # ============================================

    def get_cached_list_values(self, table, keys, values, conditions):
        return self.cache.get_list_values(table, keys, values, conditions, connection=self.open_connection)

    def set_cached_list_values(self, table, keys, values, overwrite=False):
        self.cache.set_list_values(table, keys, values, overwrite=overwrite, connection=self.open_connection)

    def set_or_update_null_cached_list_values(self, table, keys, values, conflict_constraint='id'):
        self.cache.set_or_update_null_list_values(table, keys, values, conflict_constraint=conflict_constraint, connection=self.open_connection)

    def set_cached_many(self, table, keys, data):
        self.cache.set_many_values(table, keys, data, connection=self.open_connection)

    def use_cached_many(self, table, keys, values, conditions, func, *args, **kwargs):
        internal_data = self.get_cached_list_values(table, keys, values, conditions)

        def get_external_data():
            external_data = func(*args, **kwargs)
            if not external_data:
                return
            self.set_cached_many(table, keys, external_data)
            return self.get_cached_list_values(table, keys, values, conditions)

        return internal_data or get_external_data()
