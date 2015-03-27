import sqlite3

from Task1.PrepareDBDValues import *
from Task1 import dbd
from Task1.queries import *


class SchemaToDBD:
    """
    Класс ответственнен за подключение к БД и записи схемы в таблицы.
    """

    def __init__(self, target, schema):
        """
        Метод создает подключение к БД, создает таблицы и курсор для исполнения SQL запросов.
        """
        self.connection = sqlite3.connect(target)
        # Создаем таблицы, в которые будет записана схема.
        self.connection.executescript(dbd.SQL_DBD_Init)
        self.cursor = self.connection.cursor()
        self.schema = schema

    def write_to_dbd(self):
        """
        Метод производит запись схемы в БД.
        """
        # Записываем описание схемы в БД.
        self._insert_settings()
        self.connection.executescript(QUERY_TEMP_TABLES)
        # Записываем домены и таблицы.
        self._insert_temp_domains()
        self._insert_tables()

        # Записываем поля, ограничения, индексы.
        self._insert_temp_fields()
        self._insert_temp_constraints()
        self._insert_temp_indices()

        self._insert_domains()
        self._insert_fields()
        self._insert_constraints()
        self._insert_indices()

        self.connection.executescript(QUERY_DROP_TEMP_TABLES)
        # Записываем в dbd соответсвующие изменения.
        self.connection.commit()

    def _insert_settings(self):
        """
        Метод записывает описание схемы в БД.
        """
        values = prepare_dbd_values(self.schema)
        self.cursor.executemany(QUERY_SETTINGS, values)

    def _insert_temp_domains(self):
        """
        Метод записывает домены из схемы в БД.
        """
        # Записываем данные.
        self.cursor.executemany(QUERY_TEMP_DOMAINS, [prepare_dbd_values(domain) for domain in self.schema.domains])

    def _insert_tables(self):
        """
        Метод записывает таблицы из схемы в БД.
        """
        # Записываем данные.
        self.cursor.executemany(QUERY_TABLES, [prepare_dbd_values(table) for key, table in self.schema.tables.items()])

    def _insert_temp_fields(self):
        """
        Метод записывает поля из схемы в БД.
        """
        # Собираем все записи о полях в список.
        field_values = []
        for table_key, table in self.schema.tables.items():
            # Обходим все поля таблицы. Формируем данные для записи в БД.
            for field_key, field in table.fields.items():
                # Сопоставляем именам доменов Ид_Доменов. Формируем список значений.
                values = prepare_dbd_values(field)
                # Привет, костыль.
                values[4] = self._find_domain_id(field.domain)
                values.extend([table_key])
                field_values.append(values)

        # Записываем все поля.
        self.cursor.executemany(QUERY_TEMP_FIELDS, field_values)

    def _insert_temp_constraints(self):
        """
        Метод записывает ограничения из схемы в БД.
        """
        # Собираем все записи об ограничениях в список.
        constraint_values = []
        for table_key, table in self.schema.tables.items():
            # Обходим все ограничения таблицы. Формируем данные для записи в БД.
            for key, constraint in table.constraints.items():
                values = prepare_dbd_values(constraint)
                for v in values:
                    v.append(table_key)
                constraint_values.extend(values)
        # Записываем ограничения.
        self.cursor.executemany(QUERY_TEMP_CONSTRAINTS, constraint_values)

    def _insert_temp_indices(self):
        """
        Метод записывает индексы из схемы в БД.
        """
        # Собираем все записи об индексах в список.
        indices_values = []
        for table_key, table in self.schema.tables.items():
            for iname, index in table.indices.items():
                values = prepare_dbd_values(index)
                for v in values:
                    v.append(table_key)
                indices_values.extend(values)
        # Записываем индексы.
        self.cursor.executemany(QUERY_TEMP_INDICES, indices_values)

    def _insert_domains(self):
        """
        Метод переносит домены из временной таблицы в таблицу доменов.
        """
        temp_names = domain_dbd_names()
        keys = prepare_dbd_columns(domain_dbd_names())
        column_placeholders = ', '.join(keys)
        # Динамически собираем запрос.
        for i, key in enumerate(keys):
            table = 'temp.' if key in temp_names else 'types.'
            key = 'id' if table == 'types.' and key == 'data_type_id' else key
            keys[i] = table + key
        query_select = \
            "SELECT " + ', '.join(keys) + " " \
                                          "FROM dbd$domains_temp temp " \
                                          "LEFT JOIN dbd$data_types types ON temp.data_type = types.type_id"
        query_insert = \
            "INSERT INTO dbd$domains " \
            "(" + column_placeholders + ") " + query_select
        self.cursor.execute(query_insert)

    def _insert_fields(self):
        """
        Метод переносит поля из временной таблицы в таблицу Полей.
        """
        temp_names = field_dbd_names()
        keys = field_dbd_names()
        keys.append('table_name')
        keys = prepare_dbd_columns(keys)
        column_placeholders = ', '.join(keys)
        # Динамически собираем запрос.
        for i, key in enumerate(keys):
            table = 'temp.' if key in temp_names or key == 'domain_id' else 'tables.'
            key = 'id' if table != 'temp.' else 'domain' if key == 'domain_id' else key
            keys[i] = table + key
        query_select = \
            "SELECT " + ', '.join(keys) + " " \
                                          "FROM dbd$fields_temp temp " \
                                          "LEFT JOIN dbd$tables tables ON temp.table_name = tables.name"
        query_insert = \
            "INSERT INTO dbd$fields " \
            "(" + column_placeholders + ") " + query_select
        self.cursor.execute(query_insert)

    def _insert_constraints(self):
        """
        Метод переносит ограничения из временной таблицы в таблицу Ограничений.
        """
        temp_names = constraints_dbd_names()
        keys = constraints_dbd_names()
        keys.append('table_name')
        keys = prepare_dbd_columns(keys)
        column_placeholders = ', '.join(keys)
        # Динамически собираем запрос
        for i, key in enumerate(keys):
            table = 'temp.' if key in temp_names else 'tables.'
            table = 'tables_ref.' if key == 'reference' else table
            key = 'id' if table != 'temp.' else key
            keys[i] = table + key
        query_select = \
            "SELECT " + ', '.join(keys) + " " \
                                          "FROM dbd$constraints_temp temp " \
                                          "LEFT JOIN dbd$tables tables ON temp.table_name = tables.name " \
                                          "LEFT JOIN dbd$tables tables_ref ON temp.reference = tables_ref.name "
        query_insert = \
            "INSERT INTO dbd$constraints " \
            "(" + column_placeholders + ") " + query_select
        self.cursor.execute(query_insert)
        query_select = \
            "SELECT temp.id, temp.position, fields.id " \
            "FROM dbd$constraints_temp temp " \
            "LEFT JOIN dbd$tables tables ON temp.table_name = tables.name " \
            "JOIN dbd$fields fields " \
            "WHERE tables.id = fields.table_id AND temp.items = fields.name"
        query_insert = \
            "INSERT INTO dbd$constraint_details " \
            "(constraint_id, position, field_id) " + query_select
        self.cursor.execute(query_insert)

    def _insert_indices(self):
        """
        Метод переносит индексы из временной таблицы в таблицу Индексов.
        """
        temp_names = indices_dbd_names()
        keys = indices_dbd_names()
        keys.append('table_name')
        keys = prepare_dbd_columns(keys)
        column_placeholders = ', '.join(keys)
        # Динамически собираем запрос
        for i, key in enumerate(keys):
            table = 'temp.' if key in temp_names else 'tables.'
            key = 'id' if table != 'temp.' else key
            keys[i] = table + key
        query_select = \
            "SELECT " + ', '.join(keys) + " " \
                                          "FROM dbd$indices_temp temp " \
                                          "LEFT JOIN dbd$tables tables ON temp.table_name = tables.name "
        query_insert = \
            "INSERT INTO dbd$indices " \
            "(" + column_placeholders + ") " + query_select
        self.cursor.execute(query_insert)
        query_select = \
            "SELECT temp.id, temp.position, fields.id, temp.expression, temp.descend " \
            "FROM dbd$indices_temp temp " \
            "LEFT JOIN dbd$tables tables ON temp.table_name = tables.name " \
            "JOIN dbd$fields fields " \
            "WHERE tables.id = fields.table_id AND temp.field = fields.name"
        query_insert = \
            "INSERT INTO dbd$index_details " \
            "(index_id, position, field_id, expression, descend) " + query_select
        self.cursor.execute(query_insert)

    def _find_domain_id(self, domain):
        for i, x in enumerate(self.schema.domains):
            if x == domain:
                return i + 1