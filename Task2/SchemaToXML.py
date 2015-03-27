import codecs
from collections import OrderedDict
from xml.dom.minidom import *
from xml.dom.minidom import _write_data

from Task2.PrepareXMLValues import prepare_xml_values


class SchemaToXML:
    """
    Класс ответственнен за запись схемы в  и записи схемы в таблицы.
    """

    def __init__(self, schema):
        """
        Метод создает представление XML.
        """
        self.schema = schema
        self.doc = Document()
        # Меняем функцию создания элемента, чтобы атрибуты были не в алфавитном порядке, а в порядке добавления.
        self.doc.__class__.createElement = createElement
        # Записываем данные о схеме в корень.
        self.root = self.doc.createElement("dbd_schema")
        for field in ["version", "name", "description"]:
            self.root.setAttribute(field, getattr(schema, field))

        self.doc.appendChild(self.root)
        # Записываем в XML схему.
        self._create_xml()

    def _create_xml(self):
        # Выписываем домены.
        domains = self.doc.createElement("domains")
        self.root.appendChild(domains)

        for domain in self.schema.domains:
            if domain.name:
                node = self.doc.createElement("domain")
                for key, attr in prepare_xml_values(domain).items():
                    node.setAttribute(key, str(attr))
                domains.appendChild(node)

        # Выписываем таблицы с вложенными полями, ограниениями и индексами.
        tables = self.doc.createElement("tables")
        self.root.appendChild(tables)
        for tname, table in self.schema.tables.items():
            # Выписываем данные о таблице.
            table_node = self.doc.createElement("table")
            for key, attr in prepare_xml_values(table).items():
                table_node.setAttribute(key, str(attr))
            tables.appendChild(table_node)
            # Выписываем поля.
            for fname, field in table.fields.items():
                field_node = self.doc.createElement("field")
                for key, attr in prepare_xml_values(field).items():
                    field_node.setAttribute(key, str(attr))
                table_node.appendChild(field_node)
            # Выписываем ограничения.
            for cname, constraint in table.constraints.items():
                constraint_node = self.doc.createElement("constraint")
                for key, attr in prepare_xml_values(constraint).items():
                    constraint_node.setAttribute(key, str(attr))
                table_node.appendChild(constraint_node)
            # Выписываем индексы.
            for iname, index in table.indices.items():
                index_node = self.doc.createElement("index")
                for key, attr in prepare_xml_values(index).items():
                    index_node.setAttribute(key, str(attr))
                table_node.appendChild(index_node)

    def writexml(self, target):
        """
        Метод записывает XML в файл.
        """
        with codecs.open(target, "w", "utf-8") as out:
            self.doc.writexml(out, indent="  ", addindent="  ", newl='\n', encoding="utf-8")
        self.doc.unlink()


class Elem(Element):
    def _ensure_attributes(self):
        if self._attrs is None:
            self._attrs = OrderedDict()
            self._attrsNS = OrderedDict()

    def writexml(self, writer, indent="", addindent="", newl=""):
        writer.write(indent + "<" + self.tagName)

        attrs = self._get_attributes()
        a_names = attrs.keys()
        for a_name in a_names:
            writer.write(" %s=\"" % a_name)
            _write_data(writer, attrs[a_name].value)
            writer.write("\"")
        if self.childNodes:
            writer.write(">")
            if (len(self.childNodes) == 1 and self.childNodes[0].nodeType == Node.TEXT_NODE):
                self.childNodes[0].writexml(writer, '', '', '')
            else:
                writer.write(newl)
                for node in self.childNodes:
                    node.writexml(writer, indent + addindent, addindent, newl)
                writer.write(indent)
            writer.write("</%s>%s" % (self.tagName, newl))
        else:
            writer.write("/>%s" % (newl))


def createElement(self, tagName):
    e = Elem(tagName)
    e.ownerDocument = self
    return e