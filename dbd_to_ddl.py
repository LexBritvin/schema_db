import codecs
import os
import sys
from Common.Schema import get_schema_from_dbd

from Task3.GenerateDDL import *
from Common.functions import create_file


def dbd_to_ddl(source, target):
    """
    Метод формирует в объект схемы DBD файл и записывает в SQL файл DDL представление.
    """
    schema = get_schema_from_dbd(source)
    ddl = generate_ddl(schema) + "\n" + generate_foreign_constraints(schema)
    with codecs.open(target, "w", "utf-8") as out:
        out.write(ddl)
    out.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(sys.argv)
        print('python dbd_to_xml.py <источник> <выходной sql файл>')
        exit(1)
    else:
        source = sys.argv[1]
        target = sys.argv[2]
    if source.split('.')[-1].lower() != 'dbd':
        print('Неправильно указано расширение dbd файла')
        exit(1)
    if target.split('.')[-1].lower() != 'sql':
        print('Неправильно указано расширение sql файла')
        exit(1)    
    
    if not os.path.exists(source):
        print('Файл %s не найден' % source)
        exit(1)
    create_file(target)
    dbd_to_ddl(source, target)