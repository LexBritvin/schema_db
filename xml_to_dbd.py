import os
import sys
from Common.Schema import get_schema_from_xml

from Task1.SchemaToDBD import SchemaToDBD
from Common.functions import create_file


def xml_to_dbd(source, target):
    """
    Метод формирует в объект схемы XML файл и записывает в БД.
    """
    schema = get_schema_from_xml(source)
    schema_dbd = SchemaToDBD(target, schema)
    schema_dbd.write_to_dbd()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(sys.argv)
        print('python xml_to_dbd.py <источник> <выходной xml файл>')
        exit(1)
    else:
        source = sys.argv[1]
        target = sys.argv[2]
    if source.split('.')[-1].lower() != 'xml':
        print('Неправильно указано расширение xml файла')
        exit(1)
    if target.split('.')[-1].lower() != 'dbd':
        print('Неправильно указано расширение dbd файла')
        exit(1)

    if not os.path.exists(source):
        print('Файл %s не найден' % source)
        exit(1)
    create_file(target)
    xml_to_dbd(source, target)
