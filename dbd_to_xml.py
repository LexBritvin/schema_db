import os
import sys
from Common.Schema import get_schema_from_dbd

from Task2.SchemaToXML import SchemaToXML
from Common.functions import create_file


def dbd_to_xml(source, target):
    """
    Метод формирует в объект схемы XML файл и записывает в БД.
    """
    schema = get_schema_from_dbd(source)
    schema_xml = SchemaToXML(schema)
    schema_xml.writexml(target)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(sys.argv)
        print('python dbd_to_xml.py <источник> <выходной dbd файл>')
        exit(1)
    else:
        source = sys.argv[1]
        target = sys.argv[2]
    if source.split('.')[-1].lower() != 'dbd':
        print('Неправильно указано расширение dbd файла')
        exit(1)
    if target.split('.')[-1].lower() != 'xml':
        print('Неправильно указано расширение xml файла')
        exit(1)    
    
    if not os.path.exists(source):
        print('Файл %s не найден' % source)
        exit(1)
    create_file(target)
    dbd_to_xml(source, target)