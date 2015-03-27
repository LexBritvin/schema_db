import copy


def prepare_schema_attributes(names, attributes):
    """
    Метод сопоставляет имена атрибутов в XML их именам в БД.
    Формирует полный набор атрибутов. Не указанным присваивается None.
    """
    attrs = copy.copy(attributes)
    result = dict(zip(names, [None] * len(names)))
    if 'props' in attrs:
        props = [x.strip() for x in attrs['props'].split(',')]
        props = dict(zip(props, [1] * len(props)))
        del attrs['props']
        attrs.update(props)
    xml_dbd_dictionary_ = xml_dbd_dictionary()
    for key in attrs:
        if key in xml_dbd_dictionary_:
            attrs[xml_dbd_dictionary_[key]] = attrs.pop(key)
    result.update(attrs)
    return result


def prepare_dbd_columns(names):
    dbd_dictionary_ = dbd_dictionary()
    for key, value in enumerate(names):
        names[key] = dbd_dictionary_[value] if value in dbd_dictionary_ else value
    return names


def prepare_dbd_xml_columns(names):
    xml_dictionary_ = xml_dictionary()
    for key, value in enumerate(names):
        names[key] = xml_dictionary_[value] if value in xml_dictionary_ else value
    return names


def dbd_dictionary():
    """
    Метод возвращает словарь Имя:Имя_БД.
    """
    dictionary = {
        'field': 'field_id',
        'field_name': 'field_id',
        'domain': 'domain_id',
        'data_type': 'data_type_id',
        'table_name': 'table_id',
    }

    return dictionary


def xml_dictionary():
    """
    Метод возвращает словарь Имя:Имя_БД.
    """
    dictionary = {
        'data_type': 'type_id',
        'domain': 'domain_name',
        'field': 'field_id',
        'field_name': 'field_id',
    }

    return dictionary


def xml_dbd_dictionary():
    """
    Метод возвращает словарь Имя_XML:Имя_БД.
    """
    dictionary = {
        'add': 'can_add',
        'edit': 'can_edit',
        'delete': 'can_delete',
        'rname': 'russian_short_name',
        'input': 'can_input',
        'kind': 'constraint_type',
        'type': 'data_type',
    }
    return dictionary


def domain_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Доменов.
    """
    names = [
        'name',
        'description',
        'data_type',
        'length',
        'char_length',
        'precision',
        'scale',
        'width',
        'align',
        'show_null',
        'show_lead_nulls',
        'thousands_separator',
        'summable',
        'case_sensitive',
        'abstract_domain',
    ]
    return names


def table_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Таблиц.
    """
    names = [
        'name',
        'description',
        'can_add',
        'can_edit',
        'can_delete',
        'temporal_mode',
        'access_level',
        'ht_table_flags',
        'means',
    ]
    return names


def field_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Полей.
    """
    names = [
        'position',
        'name',
        'russian_short_name',
        'description',
        'domain',
        'can_input',
        'can_edit',
        'show_in_grid',
        'show_in_details',
        'is_mean',
        'autocalculated',
        'required',
    ]
    return names


def constraints_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Ограничений.
    """
    names = [
        'name',
        'constraint_type',
        'reference',
        'unique_key_id',
        'has_value_edit',
        'cascading_delete',
        'expression',
    ]
    return names


def constraint_details_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Характеристик Ограничений.
    """
    names = [
        'items',
        'position',
    ]
    return names


def indices_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Индексов.
    """
    names = [
        'name',
        'local',
        'kind',
    ]
    return names


def index_details_dbd_names():
    """
    Метод возвращает список имен столбцов в таблице Индексов.
    """
    names = [
        'position',
        'field',
        'expression',
        'descend',
    ]
    return names


def domain_props():
    """
    Метод возвращает список имен столбцов props в таблице Доменов.
    """
    names = [
        'show_null',
        'show_lead_nulls',
        'thousands_separator',
        'summable',
        'case_sensitive',
        'abstract_domain',
    ]
    return names


def table_props():
    """
    Метод возвращает список имен столбцов props в таблице Таблиц.
    """
    names = [
        'can_add',
        'can_edit',
        'can_delete',
    ]
    return names


def field_props():
    """
    Метод возвращает список имен столбцов props в таблице Полей.
    """
    names = [
        'can_input',
        'can_edit',
        'show_in_grid',
        'show_in_details',
        'is_mean',
        'autocalculated',
        'required',
    ]
    return names


def constraint_props():
    """
    Метод возвращает список имен столбцов props в таблице Индексов.
    """
    names = [
        'has_value_edit',
        'cascading_delete',
    ]
    return names


