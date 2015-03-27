from xml.dom.minidom import *
import collections
import sqlite3


from Common import Classes
from Common.DBDColumnNames import *


def get_schema_from_xml(source):
    """
    Метод возвращает объект, содержащий объекты доменов и таблиц
    со всем входящими в них данными.
    """
    # Парсим XML и создаем объект будущей схемы.
    xml_file = parse(source)

    # Получаем данные о версии и названии схемы.
    attributes = dict(xml_file.getElementsByTagName('dbd_schema')[0].attributes.items())
    schema = Classes.Schema(attributes)

    # Формируем словарь доменов. OrderedDict использован, чтобы сохранить порядок доменов.
    domains = []
    for domain in xml_file.getElementsByTagName('domain'):
        attributes = dict(domain.attributes.items())
        attributes = prepare_schema_attributes(domain_dbd_names(), attributes)
        domains.append(Classes.Domain(attributes))

    # Формируем словарь объектов таблиц, включающий поля, их ограничения и индексы.
    tables = collections.OrderedDict()
    for table in xml_file.getElementsByTagName('table'):
        # Записываем атрибуты таблицы.
        attributes = dict(table.attributes.items())
        table_name = attributes['name']
        # Сопоставляем значения атрибутов. Формируем список атрибутов с именами столбцов.
        attributes = prepare_schema_attributes(table_dbd_names(), attributes)
        for name in ['can_edit', 'can_add', 'can_delete']:
            attributes[name] = 0 if name not in attributes else attributes[name]
        tables[table_name] = Classes.Table(attributes)

        # Записываем поля таблицы.
        fields = collections.OrderedDict()
        position = 0
        for field in table.getElementsByTagName('field'):
            position += 1
            attributes = dict(field.attributes.items())
            attributes = prepare_schema_attributes(field_dbd_names(), attributes)
            attributes['position'] = position
            if not attributes['domain']:
                domain_attributes = prepare_schema_attributes(domain_dbd_names(), attributes)
                domain_attributes['name'] = None
                domain_attributes['props'] = None
                new_domain = Classes.Domain(domain_attributes)
                field_domain = find_domain(domains, new_domain)
                attributes['domain'] = field_domain if field_domain else new_domain
                if not field_domain:
                    domains.append(new_domain)
            else:
                attributes['domain'] = find_domain_by_name(domains, attributes['domain'])
            fields[attributes['name']] = Classes.Field(attributes)

        # Записываем ограничения, наложенные на поля.
        constraints = collections.OrderedDict()
        for constraint in table.getElementsByTagName('constraint'):
            attributes = dict(constraint.attributes.items())
            attributes['items'] = [x.strip() for x in attributes['items'].split(',')]
            # Сопоставляем значения атрибутов. Формируем список атрибутов с именами столбцов.
            attributes = prepare_schema_attributes(constraints_dbd_names(), attributes)
            if 'full_cascading_delete' in attributes:
                attributes['cascading_delete'] = 1
                del attributes['full_cascading_delete']
            else:
                attributes['cascading_delete'] = 0 if 'cascading_delete' in attributes else None
            attributes['position'] = attributes['position'] if 'position' in attributes else 1
            cname = attributes['name'] if attributes['name'] else attributes['constraint_type'][0] + 'K_' + '_'.join(attributes['items'])
            constraints[cname] = Classes.Constraint(attributes)

        # Записываем индексы таблицы.
        indices = collections.OrderedDict()
        for index in table.getElementsByTagName('index'):
            attributes = dict(index.attributes.items())
            keys = indices_dbd_names()
            keys.extend(index_details_dbd_names())
            attributes = prepare_schema_attributes(keys, attributes)
            attributes['kind'] = 'U' if 'uniqueness' in attributes else 'N'
            attributes['position'] = attributes['position'] if attributes['position'] else 1
            attributes['field'] = [x.strip() for x in attributes['field'].split(',')]
            iname = attributes['name'] if attributes['name'] else 'idx_' + '_'.join(attributes['field'])
            indices[iname] = Classes.Index(attributes)

        # Записываем сформированные данные по каждой таблице в словарь с именем таблицы.
        tables[table_name].fields = fields
        tables[table_name].constraints = constraints
        tables[table_name].indices = indices

    # Формируем схему БД.
    schema.domains = domains
    schema.tables = tables

    return schema


def find_domain(domains, domain):
    """
    Метод находит в списке доменов объект домена, не учитывая имя.
    :param domains: список доменов.
    :param domain: объект домена.
    :return: объект домена в списке.
    """
    for x in domains:
        if x == domain:
            return x
    return None


def find_domain_by_name(domains, name):
    """
    Метод находит домен по имени.
    :param domains: Список доменов.
    :param name: Имя домена.
    :return: Объект домена в списке.
    """
    for x in domains:
        if x.name == name:
            return x
    return None


def get_schema_from_dbd(source):
    """
    Метод создает обхект схемы.
    :param source: База данных
    :return: Объект Schema
    """
    connection = sqlite3.connect(source)
    cursor = connection.cursor()

    # Извлекаем информацию о схеме.
    attributes = dict(cursor.execute("SELECT key, value FROM dbd$settings").fetchall())
    for old_key, new_key in \
            {'dbd.name': 'name', 'dbd.description': 'description', 'dbd.content.version': 'version'}.items():
        attributes[new_key] = attributes.pop(old_key)
    schema = Classes.Schema(attributes)

    # Извлекаем информацию о доменах.
    domains = []
    for x in cursor.execute(QUERY_DOMAINS).fetchall():
        attributes = dict(zip(domain_dbd_names(), x))
        domains.append(Classes.Domain(attributes))

    # Извлекаем информацию о таблицах.
    tables = collections.OrderedDict()
    for x in cursor.execute(QUERY_TABLES).fetchall():
        attributes = dict(zip(table_dbd_names(), x))
        tables[attributes['name']] = Classes.Table(attributes)

    # Извлекаем информацию о полях.
    keys = field_dbd_names()
    keys.append('table_name')
    for x in cursor.execute(QUERY_FIELDS).fetchall():
        attributes = dict(zip(keys, x))
        attributes['domain'] = domains[attributes['domain'] - 1]
        tables[attributes['table_name']].fields[attributes['name']] = Classes.Field(attributes)

    # Извлекаем информацию об индексах.
    keys = indices_dbd_names()
    keys.extend(index_details_dbd_names())
    keys.append('table_name')
    for x in cursor.execute(QUERY_INDICES).fetchall():
        attributes = dict(zip(keys, x))
        iname = attributes['name'] if attributes['name'] else 'idx_' + attributes['field']
        if iname in tables[attributes['table_name']].indices:
            tables[attributes['table_name']].indices[iname].details.field.append(attributes['field'])
        else:
            attributes['field'] = [attributes['field']]
            tables[attributes['table_name']].indices[iname] = Classes.Index(attributes)

    # Извлекаем информацию об ограничениях.
    keys = constraints_dbd_names()
    keys.extend(constraint_details_dbd_names())
    keys.append('table_name')
    for x in cursor.execute(QUERY_CONSTRAINTS).fetchall():
        attributes = dict(zip(keys, x))
        cname = attributes['name'] if attributes['name'] else attributes['constraint_type'][0] + 'K_' + attributes['items']
        if cname in tables[attributes['table_name']].constraints:
            tables[attributes['table_name']].constraints[cname].details.items.append(attributes['items'])
        else:
            attributes['items'] = [attributes['items']]
            tables[attributes['table_name']].constraints[cname] = Classes.Constraint(attributes)

    # Формируем схему БД.
    schema.domains = domains
    schema.tables = tables

    return schema

QUERY_DOMAINS = """
    SELECT d.name, d.description, t.type_id, d.length, d.char_length, d.precision, d.scale, d.width,
        d.align, d.show_null, d.show_lead_nulls, d.thousands_separator, d.summable, d.case_sensitive, d.abstract_domain
    FROM dbd$domains d
    LEFT JOIN dbd$data_types t ON d.data_type_id = t.id
"""

QUERY_TABLES = """
    SELECT name, description, can_add, can_edit, can_delete, temporal_mode, access_level, ht_table_flags, means
    FROM dbd$tables
"""

QUERY_FIELDS = """
    SELECT f.position, f.name, f.russian_short_name, f.description, f.domain_id, f.can_input, f.can_edit, f.show_in_grid,
        f.show_in_details, f.is_mean, f.autocalculated, f.required, t.name
    FROM dbd$fields f
    LEFT JOIN dbd$domains d ON f.domain_id = d.id
    LEFT JOIN dbd$tables t ON f.table_id = t.id
"""

QUERY_INDICES = """
    SELECT inx.name, inx.local, inx.kind, inx_d.position, f.name, inx_d.expression, inx_d.descend, t.name
    FROM dbd$indices inx
    INNER JOIN dbd$index_details inx_d ON inx.id = inx_d.index_id
    LEFT JOIN dbd$tables t ON inx.table_id = t.id
    JOIN dbd$fields f
    WHERE t.id = f.table_id AND inx_d.field_id = f.id
"""

QUERY_CONSTRAINTS = """
    SELECT c.name, c.constraint_type, tr.name, c.unique_key_id, c.has_value_edit,
        c.cascading_delete, c.expression, f.name, cd.position, t.name
    FROM dbd$constraints c
    INNER JOIN dbd$constraint_details cd ON c.id = cd.constraint_id
    LEFT JOIN dbd$tables t ON c.table_id = t.id
    LEFT JOIN dbd$tables tr ON c.reference = tr.id
    JOIN dbd$fields f
    WHERE t.id = f.table_id AND cd.field_id = f.id
"""