from collections import OrderedDict

from Common.DBDColumnNames import *


def prepare_xml_values(x):
    """
    Метод конвертирует данные из объекта в словарь на основе типа объекта.
    :param x: Объект.
    :return: словарь значений.
    """
    class_ = x.__class__.__name__
    if class_ == 'Domain':
        return domain_xml_data(x)
    elif class_ == 'Table':
        return table_xml_data(x)
    elif class_ == 'Field':
        return field_xml_data(x)
    elif class_ == 'Constraint':
        return constraint_xml_data(x)
    elif class_ == 'Index':
        return index_xml_data(x)


def domain_xml_data(x):
    values = OrderedDict()
    for key in domain_dbd_names():
        attr = getattr(x, key)
        if attr:
            if key in domain_props():
                if 'props' not in values:
                    values['props'] = []
                values['props'].append(key)
            else:
                values[key] = attr

    if 'props' in values:
        values['props'] = ', '.join(values['props'])

    return values


def table_xml_data(x):
    values = OrderedDict()
    for key in table_dbd_names():
        attr = getattr(x, key)
        if attr:
            if key in table_props():
                if 'props' not in values:
                    values['props'] = []
                values['props'].append(key.split("_")[1])
            else:
                values[key] = attr

    if 'props' in values:
        values['props'] = ', '.join(values['props'])

    return values


def field_xml_data(x):
    values = OrderedDict()

    for key in field_dbd_names():
        attr = getattr(x, key)
        if attr:
            if key in field_props():
                if 'props' not in values:
                    values['props'] = []
                key = key.split("_")[1] if key.startswith("can_") else key
                values['props'].append(key)
            elif key == 'domain':
                if getattr(attr, 'name'):
                    values[key] = getattr(attr, 'name')
                else:
                    domain = domain_xml_data(attr)
                    if 'props' in domain:
                        domain['dprops'] = domain.pop('props', None)
                    values.update(domain)
            elif key == 'position':
                continue
            else:
                key = "rname" if key == "russian_short_name" else key
                values[key] = attr

    if 'props' in values:
        values['props'] = ', '.join(values['props'])

    return values


def constraint_xml_data(x):
    values = OrderedDict()

    for key in constraints_dbd_names():
        attr = getattr(x, key)
        if attr:
            if key in constraint_props():
                if 'props' not in values:
                    values['props'] = []
                key = "full_cascading_delete" if attr == 1 else "cascading_delete"
                values['props'].append(key)
            else:
                key = "kind" if key == "constraint_type" else key
                if key == "kind":
                    attr = "PRIMARY" if attr == "P" else "FOREIGN"
                values[key] = attr
    values['items'] = ", ".join(getattr(x.details, "items"))

    if 'props' in values:
        values['props'] = ', '.join(values['props'])

    return values


def index_xml_data(x):
    values = OrderedDict()
    if x.name:
        values["name"] = x.name
    values["field"] = ", ".join(x.details.field)
    if x.kind == "U":
        values["props"] = "uniqueness"
    return values