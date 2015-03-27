import collections

import psycopg2

from Common.DBDColumnNames import *
from Common.Schema import find_domain_by_name, find_domain
from Common import Classes


def get_schema_from_postgres(database, user, password):
    """
    Метод извлекает данные Схемы из Postgres.
    """
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    schema = Classes.Schema()

    domains = []
    cursor.execute(QUERY_DOMAINS)
    # Выписываем данные о доменах.
    for domain in cursor.fetchall():
        attributes = dict(zip(['name', 'char_length', 'data_type', 'precision', 'scale'], domain))
        attributes['data_type'] = dbd_type(attributes['data_type'])
        attributes = prepare_schema_attributes(domain_dbd_names(), attributes)
        domains.append(Classes.Domain(attributes))

    tables = collections.OrderedDict()
    cursor.execute(QUERY_TABLES)
    # Выписываем данные о таблицах.
    for table in cursor.fetchall():
        attributes = dict(zip(['name'], table))
        tname = attributes['name']
        attributes = prepare_schema_attributes(table_dbd_names(), attributes)
        tables[tname] = Classes.Table(attributes)
        # Выписываем данные о полях.
        cursor.execute(QUERY_FIELDS % tname)
        fields = collections.OrderedDict()
        for field in cursor.fetchall():
            attributes = dict(zip(['name', 'autocalculated', 'domain'], [field[i] for i in range(0, 3)]))
            attributes = prepare_schema_attributes(field_dbd_names(), attributes)
            # Если домен неименованный, то создаем новый объект.
            if attributes['domain']:
                domain = find_domain_by_name(domains, attributes['domain'])
            else:
                d_attrs = dict(zip(['data_type', 'char_length', 'precision', 'scale', 'show_null'],
                                   [field[i] for i in range(3, 8)]))
                d_attrs['data_type'] = dbd_type(d_attrs['data_type'])
                d_attrs = prepare_schema_attributes(domain_dbd_names(), d_attrs)
                new_domain = Classes.Domain(d_attrs)
                domain = find_domain(domains, new_domain)
                if not domain:
                    domain = new_domain
                    domains.append(new_domain)
            attributes['domain'] = domain
            fields[attributes['name']] = Classes.Field(attributes)
        tables[tname].fields = fields

        # Выписываем данные об индексах.
        cursor.execute(QUERY_INDICES % tname)
        indices = collections.OrderedDict()
        keys = indices_dbd_names()
        keys.extend(index_details_dbd_names())
        for index in cursor.fetchall():
            attributes = dict(zip(['name', 'field', 'kind'], [index[0], [index[2]], index[3]]))
            attributes = prepare_schema_attributes(keys, attributes)
            attributes['name'] = attributes['name']
            attributes['kind'] = 'U' if attributes['kind'] else 'N'
            attributes['position'] = attributes['position'] if attributes['position'] else 1
            if index[0] in indices:
                indices[index[0]].details.field.extend(attributes['field'])
            else:
                indices[index[0]] = Classes.Index(attributes)
        tables[tname].indices = indices

        # Выписываем данные об ограничениях.
        cursor.execute(QUERY_CONSTRAINTS % tname)
        keys = constraints_dbd_names()
        keys.extend(constraint_details_dbd_names())
        constraints = collections.OrderedDict()
        for constraint in cursor.fetchall():
            attributes = dict(
                zip(['name', 'constraint_type', 'reference', 'items'], [constraint[i] for i in range(0, 4)]))
            attributes['items'] = [attributes['items']]
            attributes['constraint_type'] = attributes['constraint_type'][0]
            attributes = prepare_schema_attributes(keys, attributes)
            if constraint[0] in constraints:
                constraints[constraint[0]].details.items.extend(attributes['items'])
            else:
                constraints[constraint[0]] = Classes.Constraint(attributes)
        tables[tname].constraints = constraints

    # Формируем схему БД.
    schema.domains = domains
    schema.tables = tables
    return schema


def get_data_from_postgres(database_from, user, password):
    """
    Метод извлекает записи из таблиц.
    """
    pcon = psycopg2.connect(database=database_from, user=user, password=password)
    pcur = pcon.cursor()
    pcur.execute(QUERY_TABLES)
    tables = pcur.fetchall()
    data = collections.OrderedDict()
    for table, in tables:
        pcur.execute(SELECT_DATA_QUERY.format(table))
        data[table] = pcur.fetchall()
    pcon.close()
    return data


def dbd_type(data_type):
    """
    Метод сопоставляет имена Postgres
    """
    data_type = data_type.upper()
    data_types = {
        'CHARACTER VARYING': 'STRING',
        'NUMERIC': 'FLOAT',
        'TEXT': 'MEMO',
        'BYTEA': 'ARRAY',
        'CHARACTER': 'FIXEDCHAR',
        'REAL': 'FLOAT',
        'TSVECTOR': 'MEMO',
    }
    if 'TIMESTAMP' in data_type:
        return 'TIMESTAMP'
    return data_types[data_type] if data_type in data_types else data_type


QUERY_DOMAINS = """
SELECT domain_name, character_maximum_length, data_type, numeric_precision, numeric_scale
FROM information_schema.domains
WHERE domain_schema='public';
"""

QUERY_TABLES = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema='public' AND table_type='BASE TABLE';
"""

QUERY_FIELDS = """
SELECT column_name, column_default IS NOT NULL as autocalculated, domain_name, data_type,
    character_maximum_length, numeric_precision, numeric_scale, is_nullable = 'YES'
FROM information_schema.columns
WHERE table_name='%s'
ORDER BY ordinal_position;
"""

QUERY_INDICES = """
SELECT c.relname as "Name",
    c2.relname as "Table",
    a.attname as "Field Name",
    i.indisunique as "Unique"
FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
    JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
    LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_attribute a ON a.attrelid = c2.oid AND a.attnum = ANY(i.indkey)

WHERE c.relkind IN ('i','')
    AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
    AND pg_catalog.pg_table_is_visible(c.oid)
    AND c.relkind = 'i'
    AND NOT i.indisprimary
    AND c2.relname = '%s';
"""

QUERY_CONSTRAINTS = """
SELECT tc.constraint_name,
    tc.constraint_type,
    ccu.table_name AS references_table,
    kcu.column_name
FROM information_schema.table_constraints tc

LEFT JOIN information_schema.key_column_usage kcu
    ON tc.constraint_catalog = kcu.constraint_catalog
    AND tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_name = kcu.constraint_name

LEFT JOIN information_schema.referential_constraints rc
    ON tc.constraint_catalog = rc.constraint_catalog
    AND tc.constraint_schema = rc.constraint_schema
    AND tc.constraint_name = rc.constraint_name

LEFT JOIN information_schema.constraint_column_usage ccu
    ON rc.unique_constraint_catalog = ccu.constraint_catalog
    AND rc.unique_constraint_schema = ccu.constraint_schema
    AND rc.unique_constraint_name = ccu.constraint_name
WHERE
    tc.table_name = '%s' AND tc.constraint_type <> 'CHECK'
"""

SELECT_DATA_QUERY = "SELECT * from {0};"