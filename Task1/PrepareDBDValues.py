from Common.DBDColumnNames import *


def prepare_dbd_values(x):
    """
    Метод конвертирует данные из объекта в список на основе типа объекта.
    :param x: Объект.
    :return: Список значений.
    """
    class_ = x.__class__.__name__
    if class_ == 'Schema':
        return schema_data(x)
    elif class_ == 'Domain':
        return domain_data(x)
    elif class_ == 'Table':
        return table_data(x)
    elif class_ == 'Field':
        return field_data(x)
    elif class_ == 'Constraint':
        return constraint_data(x)
    elif class_ == 'Index':
        return index_data(x)


def schema_data(x):
    values = []
    for key, dbd_key in {'name': 'dbd.name', 'description': 'dbd.description',
                         'version': 'dbd.content.version'}.items():
        values.append([dbd_key, getattr(x, key), None])
    return values


def domain_data(x):
    values = [getattr(x, key) for key in domain_dbd_names()]
    return values


def table_data(x):
    values = [getattr(x, key) for key in table_dbd_names()]
    return values


def field_data(x):
    values = []
    for key in field_dbd_names():
        value = getattr(x, key)
        values.append(value if key != 'domain' else getattr(value, 'name'))
    return values


def constraint_data(x):
    data = []
    for item in x.details.items:
        values = [getattr(x, key) for key in constraints_dbd_names()]
        values.append(item)
        values.append(x.details.position)
        data.append(values)
    return data


def index_data(x):
    data = []
    for field in x.details.field:
        values = [getattr(x, key) for key in indices_dbd_names()]
        details = getattr(x, 'details')
        for key in index_details_dbd_names():
            if key == 'field':
                values.append(field)
            else:
                values.append(getattr(details, key))
        data.append(values)
    return data