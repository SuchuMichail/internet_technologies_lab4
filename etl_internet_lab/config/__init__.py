import pymysql

# Сообщаем Django, что у нас "правильная" версия mysqlclient
pymysql.version_info = (2, 2, 1, "final", 0)
pymysql.install_as_MySQLdb()