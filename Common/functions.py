import os


def create_file(target):
    # Если такой файл уже существует, то перезаписываем его.
    if (os.path.exists(target)):
        print("Файл %s был заменен. " % target)
        os.remove(target)
    else:
        print("Создан новый файл %s. " % target)