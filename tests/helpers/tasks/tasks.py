from sparklanes import Task, conn


@Task('mtd')
class UncacheAttribute(object):
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def mtd(self):
        self.uncache(self.attr_name)
        print('UncacheAttribute executed')


@Task('mtd')
class CacheAttribute(object):
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def mtd(self):
        self.cache(self.attr_name, 111)
        print('CacheAttribute executed')


@Task('mtd')
class ClearCache(object):
    def mtd(self):
        self.clear_cache()


@Task('mtd')
class AccessCacheAttribute(object):
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def mtd(self):
        print(getattr(self, self.attr_name))
        print('AccessCacheAttribute executed')


@Task('mtd')
class AttributeDoesNotExist(object):
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def mtd(self):
        if hasattr(self, self.attr_name):
            raise Exception('`%s` shouldn\'t be an attribute, but it is.' % self.attr_name)


@Task('mtd')
class AttributeDoesExist(object):
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def mtd(self):
        if not hasattr(self, self.attr_name):
            raise Exception('`%s` should be an attribute, but it is not.' % self.attr_name)


@Task('mtd')
class ValidTask1(object):
    def mtd(self):
        print('ValidTask1 executed')


@Task('mtd')
class ValidTask2(object):
    def mtd(self):
        print('ValidTask2 executed')


@Task('mtd')
class ValidTask3(object):
    def mtd(self):
        print('ValidTask3 executed')


@Task('mtd')
class ValidReqParams(object):
    def __init__(self, a, b, c):
        pass

    def mtd(self):
        print('ValidReqParams executed')


@Task('mtd')
class ValidOptParams(object):
    def __init__(self, a=None, b=None, c=None):
        pass

    def mtd(self):
        print('ValidOptParams executed')


@Task(entry='mtd')
class ValidReqOptParams(object):
    def __init__(self, a, b, c=None):
        pass

    def mtd(self):
        print('ValidReqOptParams executed')


@Task('mtd')
class ValidBranchTask1(object):
    def mtd(self):
        print('ValidBranchTask1 executed')


@Task('mtd')
class ValidBranchTask2(object):
    def mtd(self):
        print('ValidBranchTask2 executed')

@Task('mtd')
class ValidBranchTask3(object):
    def mtd(self):
        print('ValidBranchTask3 executed')


@Task('mtd')
class ValidBranchTask2Subtask1(object):
    def mtd(self):
        print('ValidBranchTask2Subtask1 executed')


@Task('mtd')
class ValidBranchTask2Subtask2(object):
    def mtd(self):
        print('ValidBranchTask2Subtask2 executed')


class UndecoratedTask(object):
    def mtd(self):
        print('UndecoratedTask executed')


@Task('mtd')
class ExceptionThrowingTask(object):
    def mtd(self):
        raise Exception


@Task('mtd')
class ChangeContext(object):
    def mtd(self):
        conn.set_sc()


@Task('mtd')
class UseContext(object):
    def mtd(self):
        conn.sc.range(3)