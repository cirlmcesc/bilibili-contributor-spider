# -*- coding: utf-8 -*-

""" mysqldb connect package Pedoo """

import random
import time
try:
    import MySQLdb
except ImportError:
    import pymysql as MySQLdb

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "passwd": "",
    "db": "bilibili",
    "charset": "utf8"
}

def CheckConnect(func):
    def execute(*args, **kw):
        if isinstance(MySQLConnect.db_connect, dict) or MySQLConnect.execute_count > 100:
            MySQLConnect.connect(MYSQL_CONFIG)
            MySQLConnect.execute_count = 0
        return func(*args, **kw)
    return execute


class MySQLConnect(object):
    """ MySQL connect"""
    execute_count = 0
    sql_statement_log = []
    db_connect = {}
    db = {}

    @classmethod
    @CheckConnect
    def getDB(cls):
        return cls.db

    @classmethod
    @CheckConnect
    def getDBConnect(cls):
        return cls.db_connect

    @classmethod
    def connect(cls, db_config):
        """ set db connect, example:
            db_config = {
                "host": "127.0.0.1",
                "user": "root",
                "passwd": "",
                "db": "schema",
                "charset": "utf-8"
            }
        """
        cls.db_connect = MySQLdb.connect(**db_config)
        cls.db = cls.db_connect.cursor(cursorclass=MySQLdb.cursors.DictCursor)

    @classmethod
    @CheckConnect
    def execute(cls, sql, parameter=(), many=False):
        """ execute sql """
        cls.sql_statement_log.append({time.time(): sql})
        cls.execute_count += 1

        try:
            if many:
                cls.db.executemany(sql, parameter)
            else:
                cls.db.execute(sql, parameter)

            if sql.upper().replace(" ", "").startswith("SELECT"):
                return cls.db.fetchall()
        except:
            print cls.db._last_executed

        return cls.db_connect.commit()

    @classmethod
    def log(cls):
        """ sql execute log"""
        return cls.sql_statement_log


class QueryBuilder(object):
    """ Query Builder """

    def __init__(self, ormmodel):
        self._ormmodel = ormmodel
        self._table_name = ormmodel.table_name
        self._fields = "*"
        self._join = []
        self._where = []
        self._order = []
        self._group = []
        self._limit = ""
        self._final_sql = ""

    def select(self, *fields):
        """ set select fields """
        if not len(fields):
            self._fields = "*"
        elif len(fields) > 0:
            self._fields = "id, " + (", ".join(fields))

        return self

    def BuildQuerySQLString(self):
        """ build sql string """
        if isinstance(self._fields, list):
            field = tuple(self._fields).__str__().replace('(', '')
            field = field.replace(')', '')
        else:
            field = self._fields

        sql = "SElECT %s FROM %s" % (field.replace("'", ''), self._table_name)

        return sql + "".join(
            map(lambda condition: self._buildConditionString(condition), [
                "_join", "_where", "_order", "_limit", "_group"
            ]))

    def _buildConditionString(self, condition):
        """ build condition string """
        def transConditionString():
            """ transformation condition string """
            if condition == "_order":
                return " ORDER BY "
            elif condition == "_group":
                return " GROUP BY "

            return "%s " % condition.replace("_", " ").upper()

        content = getattr(self, condition)
        joinstring = ", " if condition == "_order" or condition == "_group" else " "
        startstring =  " " if condition == "join" else transConditionString()

        if isinstance(content, str):
            return content

        return (startstring + joinstring.join(content)) if len(content) else ""

    def join(self, table, current_table_field, target_table_field, **kw):
        """ join """
        self._join.append("%s %s ON %s = %s" % (
            "INNER JOIN" if not len(kw) else kw["_cmp"],
            table, current_table_field, target_table_field))
        return self

    def leftJoin(self, table, current_table_field, target_table_field):
        """ left join """
        return self.join(table, current_table_field,
                         target_table_field, _cmp="LEFT JOIN")

    def rightJogin(self, table, current_table_field, target_table_field):
        """ right join """
        return self.join(table, current_table_field,
                         target_table_field, _cmp="RIGHT JOin")

    def where(self, field, *args, **kw):
        """ where """

        if len(self._where) > 0 and self._where[-1] != "AND" and self._where[-1] != "OR":
            self._where.append("AND" if not len(kw) else kw['_cmp'])

        if hasattr(field, "__call__"):
            builder = field(QueryBuilder(self._ormmodel))
            self._where.append("(%s)" % builder._buildConditionString("_where").replace(" WHERE ", ""))
        elif isinstance(field, list):
            for condition in field:
                self.where(*condition)
        else:
            _cmp = "=" if len(args) == 1 else args[0]
            value = args[0] if len(args) == 1 else args[1]
            restring = value.replace(" ", "") if isinstance(value, str) else str(value)

            if (not isinstance(value, int) or
                not restring.startswith("(") or
                not restring.startswith("'") or
                not restring.startswith("NULL")):
                value = "'%s'" % value

            self._where.append(" ".join((field, _cmp, value)))

        return self

    def orWhere(self, field, *args):
        """ or where """
        return self.where(field, *args, _cmp="OR")

    def whereIn(self, field, _tuple, **kw):
        """ where in """
        _cmp = "IN" if not len(kw) else kw["_cmp"]
        return self.where(field, _cmp, "('%s')" % ("' ,'".join(_tuple)))

    def whereNotIn(self, field, _tuple):
        """ where not in """
        return self.whereIn(field, _tuple, _cmp="NOT IN")

    def whereBetween(self, field, from_condition, to_condition, **kw):
        """ where between """
        between = "BETWEEN" if not len(kw) else kw["_cmp"]

        return self.where(field, between, "'%s' AND '%s'" % (from_condition, to_condition))

    def whereNotBetween(self, field, from_condition, to_condition):
        """ where not between """
        return self.whereBetween(field, from_condition, to_condition, _cmp="NOT BETWEEN")

    def whereNull(self, field, **kw):
        """ where field null """
        _cmp = "IS" if not len(kw) else kw["_cmp"]

        return self.where(field, _cmp, "NUll")

    def whereNotNull(self, field):
        """ where field is not null"""
        return self.whereNull(field, _cmp="IS NOT")

    def limit(self, offset, *args):
        """ limit """
        offset = 0 if not len(args) else offset
        number = offset if not len(args) else args[0]
        self._limit = "%d, %d" % (offset, number)

        return self

    def orderBy(self, field, *args):
        """ order by """
        _cmp = "ASC" if not len(args) else "DESC"
        self._order.append(" ".join((field, _cmp)))

        return self

    def groupBy(self, field):
        """ group by """
        if isinstance(field, str):
            self._group.append(field)
        elif isinstance(field, list):
            self._group = field

        return self

    def update(self, attributes):
        """ execute update """
        source_fields = ["%s = %s" % (attr, "%s") for attr in attributes]
        srouce_value = [attributes.get(attr, '') for attr in attributes]
        sql = ("UPDATE %s SET " % self._table_name) + ", ".join(source_fields)
        sql = sql + self._buildConditionString("_where")

        return MySQLConnect.execute(sql, parameter=srouce_value)

    def delete(self):
        """ execute delete """
        sql = "DELETE FROM %s" % self._table_name
        sql = sql + self._buildConditionString("_where")

        return MySQLConnect.execute(sql)

    def get(self):
        """ query data """
        return ResaultBuilder.query(self)

    def first(self):
        """ query first one col data """
        return ResaultBuilder.query(self, few=False)

    def has(self):
        """ check has """
        return len(ResaultBuilder.query(self)) > 0


class ResaultBuilder(object):
    """ Resault Builder """

    @classmethod
    def execute(cls, sql, parameter=(), many=False):
        """ execute """
        return MySQLConnect.execute(sql, parameter=parameter, many=False)

    @classmethod
    def query(cls, builder, few=True):
        """ query """
        resault = MySQLConnect.execute(builder.BuildQuerySQLString())

        if not len(resault):
            return [] if few else None

        if few:
            return list(map(
                lambda attributes:cls.buildORMModelInstance(builder._ormmodel, attributes), resault))
        else:
            return cls.buildORMModelInstance(builder._ormmodel, resault[0])

    @classmethod
    def buildORMModelInstance(cls, model, attributes):
        """ build model instance """
        return model(origin_attributes=attributes)


class ORMModel(object):
    """ Model """
    no_attribute = (
        "fields", "table_name", "_ORMModel__id", "origin_attributes", "origin_id")

    def __init__(self, table_name="", attributes={}, origin_attributes={}):
        if not isinstance(attributes, dict) or not isinstance(origin_attributes, dict):
            raise AttributeError("data type error.")

        self.fields = []

        if self.table_name == "" and table_name == "":
            self.table_name = self.__class__.__name__.lower()
        elif table_name != "":
            self.table_name = table_name

        def setModelInstanceAttribute(attributedata):
            for val in attributedata:
                self.__setattr__(val, attributedata.get(val, None))

        if isinstance(attributes, dict) and len(attributes):
            setModelInstanceAttribute(attributes)

        if isinstance(origin_attributes, dict) and len(origin_attributes):
            self.origin_attributes = origin_attributes
            self.origin_id = origin_attributes.get("id", "")
            setModelInstanceAttribute(origin_attributes)
        else:
            self.origin_attributes = origin_attributes
            self.origin_id = 0

    def __setattr__(self, attr, value):
        if not attr in self.no_attribute and not attr in self.fields:
            fields = self.fields
            fields.append(attr)
            self.__dict__["fields"] = fields

        self.__dict__[attr] = value

    def __getattribute__(self, attr):
        new_instance = super(ORMModel, self)

        if attr == "table_name" and new_instance.__getattribute__("table_name") == "":
            return new_instance.__class__.__name__.lower()

        try:
            return new_instance.__getattribute__(attr)
        except AttributeError:
            return ""

    @classmethod
    def get(cls):
        """ get all data """
        return cls.all()

    @classmethod
    def all(cls):
        """ get all data """
        return ResaultBuilder.query(QueryBuilder(cls))

    @classmethod
    def first(cls):
        """ get first data """
        return ResaultBuilder.query(QueryBuilder(cls).limit(1), few=False)

    @classmethod
    def update(cls, data):
        """ update data """
        return QueryBuilder(cls).update(data)

    @classmethod
    def insert(cls, data):
        """ attribute insert to table"""
        if (not isinstance(data, list) and
            not isinstance(data, dict)) or not len(data):
            raise AttributeError("data type error.")

        def BuildInsertValueParam():
            """ build param """
            def builddata(value):
                return tuple([value.get(field, '') for field in value])

            return builddata(data) if isinstance(data, dict) else [
                builddata(instance) for instance in data
            ]

        fields = data.keys() if isinstance(data, dict) else random.choice(data).keys()
        sql = "INSERT INTO %s %s VALUES %s" % (
            cls.table_name,
            tuple(fields).__str__().replace("'", ''),
            "(%s)" % ", ".join(["%s" for index in range(len(fields))]))

        return MySQLConnect.execute(
            sql, parameter=BuildInsertValueParam(), many=isinstance(data, list))

    @classmethod
    def find(cls, data_id):
        """ get the id data """
        return QueryBuilder(cls).where('id', data_id).first()

    @classmethod
    def select(cls, *fields):
        """ select """
        return QueryBuilder(cls).select(*fields)

    @classmethod
    def where(cls, field, *args, **kw):
        """ new QueryBuilder and .where """
        return QueryBuilder(cls).where(field, *args, **kw)

    @classmethod
    def has(cls, field, *args, **kw):
        """ check has """
        return QueryBuilder(cls).where(field, *args, **kw).has()

    @classmethod
    def query(cls, sql):
        """ execute sql string """
        return MySQLConnect.execute(sql)

    @staticmethod
    def DB():
        """ get MySQLConnect.db """
        return MySQLConnect.getDB()

    @staticmethod
    def log():
        """ get MySQLConnect.sql_statement_log """
        return MySQLConnect.sql_statement_log

    def arrangeAttributes(self):
        """ arrange self attribute """
        attributes = dict().fromkeys(self.fields)

        for attr in attributes:
            if attr != '_ORMModel__id':
                attributes[attr] = getattr(self, attr)

        return attributes

    def save(self):
        """ update model attribute change """
        attributes = self.arrangeAttributes()

        if len(self.origin_attributes):
            QueryBuilder(self).where("id", self.origin_id).update(attributes)

            return self.find(self.origin_id)

        return self.insert(attributes)

    def delete(self):
        """ delete this model data """
        if self.origin_id == 0:
            raise AttributeError("Instance have not attribute 'ID'")

        return QueryBuilder(self).where("id", self.origin_id).delete()

    def dict(self):
        """ self to dict """
        d = {}

        for field in self.fields:
            d[field] = getattr(self, field)

        return d
