from Common.DBDColumnNames import *

QUERY_TEMP_TABLES = """\
        CREATE TABLE dbd$domains_temp (
            id integer PRIMARY KEY autoincrement default(NULL),
            name varchar unique default(null),
            description varchar default(NULL),
            data_type varchar NOT NULL,
            length integer default(NULL),
            char_length integer default(NULL),
            precision integer default(NULL),
            scale integer default(NULL),
            width integer default(NULL),
            align char default(NULL),
            show_null boolean default(NULL),
            show_lead_nulls boolean default(NULL),
            thousands_separator boolean default(NULL),
            summable boolean default(NULL),
            case_sensitive boolean default(NULL),
            abstract_domain boolean default(NULL)
        );

        create table dbd$fields_temp (
            id integer primary key autoincrement default(null),
            table_name varchar not null,
            position integer not null,
            name varchar not null,
            russian_short_name varchar not null,
            description varchar default(null),
            domain varchar,
            can_input boolean default(null),
            can_edit boolean default(null),
            show_in_grid boolean default(null),
            show_in_details boolean default(null),
            is_mean boolean default(null),
            autocalculated boolean default(null),
            required boolean default(null)
        );

        create table dbd$constraints_temp (
            id integer primary key autoincrement default (null),
            table_name varchar not null,
            name varchar default(null),
            constraint_type char default(null),
            reference integer default(null),
            unique_key_id integer default(null),
            has_value_edit boolean default(null),
            cascading_delete boolean default(null),
            expression varchar default(null),

            position integer not null,
            items varchar not null default(null)
        );

        create table dbd$indices_temp (
            id integer primary key autoincrement default(null),
            table_name varchar not null,
            name varchar default(null),
            local boolean default(0),
            kind char default(null),

            position integer not null,
            field varchar default(null),
            expression varchar default(null),
            descend boolean default(null)
        );
"""

QUERY_DROP_TEMP_TABLES = """\
    DROP TABLE dbd$domains_temp;
    DROP TABLE dbd$fields_temp;
    DROP TABLE dbd$constraints_temp;
    DROP TABLE dbd$indices_temp;
"""

QUERY_SETTINGS = "INSERT INTO dbd$settings (KEY, value, valueb) VALUES (?, ?, ?)"

QUERY_TEMP_DOMAINS = "INSERT INTO dbd$domains_temp (%s) VALUES (%s)" % (
    ', '.join(domain_dbd_names()), ', '.join(['?'] * len(domain_dbd_names())))

QUERY_TABLES = "INSERT INTO dbd$tables (%s) VALUES (%s)" % (
    ', '.join(table_dbd_names()), ', '.join(['?'] * len(table_dbd_names())))

QUERY_TEMP_FIELDS = "INSERT INTO dbd$fields_temp (%s, table_name) VALUES (%s, ?)" % (
    ', '.join(field_dbd_names()), ', '.join(['?'] * len(field_dbd_names())))

keys = constraints_dbd_names()
keys.extend(constraint_details_dbd_names())
QUERY_TEMP_CONSTRAINTS = "INSERT INTO dbd$constraints_temp (%s, table_name) VALUES (%s, ?)" % (
    ', '.join(keys), ', '.join(['?'] * len(keys)))

keys = indices_dbd_names()
keys.extend(index_details_dbd_names())
# Подготавливаем запрос.
QUERY_TEMP_INDICES = "INSERT INTO dbd$indices_temp (%s, table_name) VALUES (%s, ?)" % (
    ', '.join(keys), ', '.join(['?'] * len(keys)))

QUERY_INSERT_CONSTRAINT_DETAILS = \
    "INSERT INTO dbd$constraint_details (constraint_id, position, field_id) VALUES (?, ?, ?)"

QUERY_INSERT_INDEX_DETAILS = \
    "INSERT INTO dbd$index_details (index_id, position, field_id) VALUES (?, ?, ?)"