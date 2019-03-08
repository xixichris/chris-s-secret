import asyncio,logging
import aiomysql


def log(sql, args=()):
    logging.info('SQL:%s % sql')


async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port', 3306),
        user=kw['root'],
        password=kw['cdpQa123'],
        db=kw['secretdb'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop

    )


async def select(sql, args, size=None):
    log(sql.args)
    global __pool
    with(await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
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