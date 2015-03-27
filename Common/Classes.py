import collections


class Schema:
    """
    Класс Схемы. Содержит в себе все объекты Таблиц и Доменов.
    """

    def __init__(self, attributes=None):
        self.name = attributes['name'] if attributes else None
        self.description = attributes['description'] if attributes else None
        self.version = attributes['version'] if attributes else None
        self.domains = []
        self.tables = collections.OrderedDict()


class Domain:
    """
    Класс хранит значения атрибутов Домен из XML в виде словаря.
    """

    def __init__(self, attributes):
        self.name = attributes['name']
        self.description = attributes['description']
        self.data_type = attributes['data_type']
        self.length = attributes['length']
        self.char_length = attributes['char_length']
        self.precision = attributes['precision']
        self.scale = attributes['scale']
        self.width = attributes['width']
        self.align = attributes['align']
        self.show_null = attributes['show_null']
        self.show_lead_nulls = attributes['show_lead_nulls']
        self.thousands_separator = attributes['thousands_separator']
        self.summable = attributes['summable']
        self.case_sensitive = attributes['case_sensitive']
        self.abstract_domain = attributes['abstract_domain']

    def __eq__(self, other):
        for key, value in self.__dict__.items():
            if key != 'name':
                if value != other.__dict__[key]:
                    return False
        return True


class Table:
    """
    Класс хранит значения атрибутов Таблиц из XML в виде словаря.
    Таблица хранит в себе все поля, индексы и ограничения.
    """

    def __init__(self, attributes):
        self.name = attributes['name']
        self.description = attributes['description']
        self.can_add = attributes['can_add']
        self.can_edit = attributes['can_edit']
        self.can_delete = attributes['can_delete']
        self.temporal_mode = attributes['temporal_mode']
        self.access_level = attributes['access_level']
        self.ht_table_flags = attributes['ht_table_flags']
        self.means = attributes['means']

        self.fields = collections.OrderedDict()
        self.indices = collections.OrderedDict()
        self.constraints = collections.OrderedDict()


class Field:
    """
    Класс хранит значения атрибутов Полей из XML в виде словаря.
    """

    def __init__(self, attributes):
        self.name = attributes['name']
        self.position = attributes['position']
        self.russian_short_name = attributes['russian_short_name']
        self.description = attributes['description']
        self.domain = attributes['domain']
        self.can_input = attributes['can_input']
        self.can_edit = attributes['can_edit']
        self.show_in_grid = attributes['show_in_grid']
        self.show_in_details = attributes['show_in_details']
        self.is_mean = attributes['is_mean']
        self.autocalculated = attributes['autocalculated']
        self.required = attributes['required']


class Constraint:
    """
    Класс хранит значения атрибутов Ограничений из XML в виде словаря.
    """

    def __init__(self, attributes):
        self.name = attributes['name']
        self.constraint_type = attributes['constraint_type'][0].upper()
        self.reference = attributes['reference']
        self.unique_key_id = attributes['unique_key_id']
        self.has_value_edit = attributes['has_value_edit']
        self.cascading_delete = attributes['cascading_delete']
        self.expression = attributes['expression']
        self.details = ConstraintDetails(attributes['items'], attributes['position'])


class ConstraintDetails:
    """
    Класс хранит значения атрибутов Ограничений из XML в виде словаря.
    """

    def __init__(self, items, position):
        self.items = items
        self.position = position


class Index:
    """
    Класс хранит значения атрибутов Индексов из XML в виде словаря.
    """

    def __init__(self, attributes):
        self.name = attributes['name']
        self.local = attributes['local']
        self.kind = attributes['kind']
        self.details = IndexDetails(attributes)


class IndexDetails:
    """
    Класс хранит значения атрибутов Индексов из XML в виде словаря.
    """

    def __init__(self, attributes):
        self.position = attributes['position']
        self.field = attributes['field']
        self.expression = attributes['expression']
        self.descend = attributes['descend']