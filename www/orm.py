import asyncio,logging
import aiomysql


def log(sql, args=()):
    logging.info('SQL:%s' % sql)

#创建一个全局的连接池，每个ＨＴＴＰ请求都可以从连接池直接获取数据库连接
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool #连接池由全局变量__pool存储
    __pool = await aiomysql.create_pool(
        host=kw.get('host','localhost'), #host是关键字参数，localhost是其默认值
        port=kw.get('port', 3306),
        user=kw['user'], #chris
        password=kw['password'], #cdpQa123
        db=kw['db'], #secretdb
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),#默认自动提交事务
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop

    )


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with(await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        #查询的返回格式会变成字典格式
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        #如果传入ｓｉｚｅ参数，则用fetchamany获取指定数量的记录，未传入的话使用fetchall获取所有记录
        await cur.close()
        logging.info('rows returned:%s' % len(rs))
        return rs


async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)


class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

    def __init__(self, name=None, primary_key = False, default=None, ddl='varchar(100'):
        super().__init__(name, ddl, primary_key, default)