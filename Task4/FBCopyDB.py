import copy

import fdb


def fb_execute_ddl(database, masterkey, ddl):
    """
    Метод создает БД в Firebird и исполняет ddl файл.
    """
    con = fdb.create_database("create database '{0}' user 'SYSDBA' password '{1}';".format(database, masterkey))
    cur = con.cursor()
    ddl = ddl.replace("\n", '')
    # Построчно выполняем SQL запросы, так как драйвер FB не позволяет выполнить за один.
    for row in ddl.split(";"):
        if row:
            cur.execute(row)
    con.commit()
    con.close()


def fb_insert_data(database, masterkey, data):
    """
    Метод заполняет таблицы БД.
    """
    fcon = fdb.connect(database=database, user='SYSDBA', password=masterkey)
    fcur = fcon.cursor()
    for table, values in data.items():
        if values:
            query = INSERT_DATA_QUERY % {
                'table': table,
                'values': ', '.join(('?',) * len(values[0]))
            }
            fcur.executemany(query, values)
    fcon.commit()
    fcon.close()


def fb_alter_foreign_keys(database, masterkey, ddl):
    """
    Метод создает внешние ключи.
    """
    con = fdb.connect(dsn=database, user='SYSDBA', password=masterkey)
    cur = con.cursor()
    ddl = ddl.replace("\n", '')
    # Построчно выполняем SQL запросы, так как драйвер FB не позволяет выполнить за один.
    for row in ddl.split(";"):
        if row:
            cur.execute(row)
    con.commit()
    con.close()


def convert_postgres_to_fb_schema(schema):
    """
    Метод преобразует схему, полученную из PostgreSQL, в представление схемы в FB.
    """
    # Копируем объект схемы, чтобы производить изменения не на основном объекте.
    fbschema = copy.deepcopy(schema)
    # Так как FB не позволяет имена выше 31 символа, все имена обрезаются.
    for domain in fbschema.domains:
        domain.name = domain.name[0:31] if domain.name else None
        domain.data_type = fb_data_type(domain.data_type)

    for tname, table in fbschema.tables.items():
        table.name = table.name[0:31]
        for fname, field in table.fields.items():
            field.name = field.name[0:31]
        for iname, index in table.indices.items():
            index.name = index.name[0:31]
        for cname, contraint in table.constraints.items():
            contraint.name = contraint.name[0:31]

    return fbschema


def fb_data_type(data_type):
    """
    Метод сопоставляет типам Схемы типы Firebird.
    """
    data_types = {'STRING': 'VARCHAR',
                  'MEMO': 'VARCHAR(1000)',
                  'FIXEDCHAR': 'CHAR',
                  'ARRAY': 'VARCHAR(1000)',
                  'BOOLEAN': 'SMALLINT ',
    }
    return data_types[data_type] if data_type in data_types else data_type


INSERT_DATA_QUERY = "INSERT INTO %(table)s VALUES (%(values)s);"