import argparse
import getpass
import os


from Task3.GenerateDDL import *
from Task4.FBCopyDB import *
from Task4.PostgresGetDB import *


def postgres_to_firebird(database_from, username, password, database_to, masterkey):
    if os.path.exists(database_to):
        os.remove(database_to)
    schema = get_schema_from_postgres(database_from, username, password)
    data = get_data_from_postgres(database_from, username, password)
    fbschema = convert_postgres_to_fb_schema(schema)
    fb_execute_ddl(database_to, masterkey, generate_ddl(fbschema))
    fb_insert_data(database_to, masterkey, data)
    fb_alter_foreign_keys(database_to, masterkey, generate_foreign_constraints(fbschema))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_postgres', '-dpg', help='База данных источник Postgres', required=True)
    parser.add_argument('--database_firebird', '-dfb', help='База данных цель Firebird', required=True)
    parser.add_argument('--user', '-u', help='Имя пользователя')
    parser.add_argument('--password', '--pass', '-p', help='Пароль')
    parser.add_argument('--fbpass', '-f', help='Пароль SYSDBA Firebird')

    args = parser.parse_args()
    db_from = args.database_postgres
    db_to = args.database_firebird
    if args.user:
        user = args.user
    else:
        user = 'postgres'

    if args.password:
        password = args.password
    else:
        password = getpass.getpass('Введите пароль для PostgreSQL: ')

    if args.fbpass:
        masterkey = args.fbpass
    else:
        masterkey = getpass.getpass('Введите пароль для Firebird: ')

    postgres_to_firebird(db_from, user, password, db_to, masterkey)

    print('Перенос данных из PostgreSQL в Firebird завершен.')