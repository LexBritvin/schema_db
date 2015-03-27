create_table_sql = "CREATE %(temp)s TABLE %(name)s (%(columns)s);\n"
create_domain_sql = "CREATE DOMAIN %(name)s AS %(type)s%(length)s;\n"
create_index_sql = "CREATE %(unique)s INDEX %(name)s ON %(table)s (%(columns)s);\n"
constraint_sql = "CONSTRAINT %(name)s %(type)s KEY (%(items)s) %(reference)s"
foreign_constraint_sql = "ALTER TABLE %(table)s ADD %(constraint)s;\n"
reference_sql = "REFERENCES {0} ({1})"
column_sql = '%(name)s %(type)s%(length)s %(constraint)s'


def generate_ddl(schema):
    """
    Метод формирует DDL представление схемы.
    :param schema: Объект схемы.
    :return: Строку DDL
    """
    ddl = ""
    # Формируем записи о доменах.
    for domain in schema.domains:
        if domain.name:
            ddl += create_domain_sql % {
                'name': domain.name,
                'type': domain.data_type,
                'length': '({0})'.format(domain.char_length) if domain.char_length else '',
            }
    # Формируем записи о таблицах, полях, индексах и ограничениях.
    for tname, table in schema.tables.items():
        columns = []
        for fname, field in table.fields.items():
            length = []
            if field.domain.char_length:
                length.append(str(field.domain.char_length))
            if field.domain.scale:
                length.append(str(field.domain.scale))
            length = ', '.join(length)
            column_values = {
                'name': fname,
                'type': field.domain.name if field.domain.name else field.domain.data_type,
                'constraint': '' if field.domain.show_null else 'NOT NULL',
                'length': '({0})'.format(length) if field.domain.char_length and not field.domain.name else '',
            }
            columns.append(column_sql % column_values)

        for cname, constraint in table.constraints.items():
            if constraint.constraint_type != 'F':
                items = [constraint.details.items]

                constraint = constraint_sql % {
                    'name': constraint.name if constraint.name else cname,
                    'type': 'PRIMARY' if constraint.constraint_type == 'P' else 'FOREIGN',
                    'items': ', '.join(constraint.details.items),
                    'reference': reference_sql.format(constraint.reference,
                                                      ', '.join(items)) if constraint.reference else ''
                }
                columns.append(constraint)

        table_values = {
            'temp': 'TEMP' * (table.temporal_mode is True),
            'name': table.name,
            'columns': ', '.join(columns),
        }
        ddl += create_table_sql % table_values
        for indname, index in table.indices.items():
            ddl += create_index_sql % {
                'unique': 'UNIQUE' * (index.kind == 'U'),
                'name': index.name if index.name else indname,
                'table': tname,
                'columns': ', '.join(index.details.field)
            }

    return ddl


def generate_foreign_constraints(schema):
    """
    Метод формирует DDL представление схемы для внешних ключей.
    :param schema: Объект схемы.
    :return: Строку DDL
    """
    ddl = ''
    for tname, table in schema.tables.items():
        for cname, constraint in table.constraints.items():
            if constraint.constraint_type == 'F':
                ddl += foreign_constraint_sql % {
                    'table': table.name,
                    'constraint': constraint_sql %
                                  {'name': constraint.name if constraint.name else cname,
                                   'type': 'PRIMARY' if constraint.constraint_type == 'P' else 'FOREIGN',
                                   'items': ', '.join(constraint.details.items),
                                   'reference': reference_sql.format(constraint.reference,
                                                                     ', '.join(
                                                                         constraint.details.items)) if constraint.reference else ''}

                }
    return ddl