import os
from Common.Classes import Schema

from Task1.SchemaToDBD import *
from Task2.SchemaToXML import *
from Task3.GenerateDDL import *
from Task4.PostgresGetDB import *
from Task4.FBCopyDB import *
from Common.functions import create_file


def __main__():
    """
    source = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) == 3 else os.path.splitext(source)[0] + '.dbd'
    """
    # """
    # source = 'prjadm.xml'
    # target = 'prjadm2.dbd'
    # target_xml = 'prjadm2.xml'
    # """
    # xml_to_dbd(source, target)
    # dbd_to_xml(target, target_xml)

    # source_ddl = 'prjadm.dbd'
    # target_ddl = 'prjadm.ddl'
    # dbd_to_ddl(source_ddl, target_ddl)
    postgres_to_firebird('pagila', 'postgres', '123', 'pagila.fdb', 'masterkey')


def xml_to_dbd(source, target):
    """
    Метод формирует в объект схемы XML файл и записывает в БД.
    """
    schema = Schema.get_schema_from_xml(source)
    create_file(target)
    schema_dbd = SchemaToDBD(target, schema)
    schema_dbd.write_to_dbd()


def dbd_to_xml(source, target):
    """
    Метод формирует в объект схемы XML файл и записывает в БД.
    """
    schema = Schema.get_schema_from_dbd(source)
    create_file(target)
    schema_xml = SchemaToXML(schema)
    schema_xml.writexml(target)


def dbd_to_ddl(source, target):
    """
    Метод формирует в объект схемы DBD файл и записывает в SQL файл DDL представление.
    """
    schema = Schema.get_schema_from_dbd(source)
    ddl = generate_ddl(schema) + "\n" + generate_foreign_constraints(schema)
    create_file(target)
    with codecs.open(target, "w", "utf-8") as out:
        out.write(ddl)
    out.close()


def postgres_to_firebird(database_from, username, password, database_to, masterkey):
    if os.path.exists(database_to):
        os.remove(database_to)
    schema = get_schema_from_postgres(database_from, username, password)
    schema_xml = SchemaToXML(schema)
    schema_xml.writexml('test.xml')
    data = get_data_from_postgres(database_from, username, password)
    fbschema = convert_postgres_to_fb_schema(schema)
    fb_execute_ddl(database_to, masterkey, generate_ddl(fbschema))
    fb_insert_data(database_to, masterkey, data)
    fb_alter_foreign_keys(database_to, masterkey, generate_foreign_constraints(fbschema))


# Точка входа в программу.
__main__()