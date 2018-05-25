from sparklanes import Task

from sparklanes.framework.task_core import LaneTask
from sparklanes.framework.utils import _validate_params
from sparklanes.spark import get_logger

@Task(entry='method_a')
class ClsTest(object):
    """This is a docstring"""

    def __init__(self, c, d):
        self.c = c
        self.d = d

    def method_a(self):
        self.cache('df', 10000)
        self.cache('abc', 'ABC')
        print('ClsTest.method_a ' + self.method_b())
        print('SELF.C', self.c)

    def method_b(self):
        return 'executed'


@Task(entry='method_c')
class AnotherClassTest(object):
    def __init__(self, c, d):
        self.c = c
        self.d = d

    def method_c(self):
        print('AnotherClassTest.method_a ' + self.method_b())
        print('SELF.DF', self.df)
        print('SELF.ABC', self.abc)
        print('accessing again?')
        print(self.c)

    def method_b(self):
        return 'executed'


@Task('mtd')
class Branch1Task(object):
    def mtd(self):
        print('Branch1Task executed.')


@Task('mtd')
class Branch2Task1(object):
    def mtd(self):
        print('Branch2Task1 executed')


@Task('mtd')
class Branch2Task2(object):
    def mtd(self):
        print('Branch2Task2 executed')


@Task('mtd')
class Branch2Task2SubTask1(object):
    def mtd(self):
        print('Branch2Task2SubTask1 executed')


@Task('mtd')
class Branch2Task2SubTask2(object):
    def mtd(self):
        print('Branch2Task2SubTask2 executed')


@Task('mtd')
class Branch3Task1(object):
    def mtd(self):
        print('Branch3Task1 executed')


@Task('mtd')
class Branch3Task2(object):
    def mtd(self):
        print('Branch3Task2 executed')


@Task('method_d')
class YetAnotherClass(object):
    def __init__(self, c, d):
        self.c = c
        self.d = d

    def method_d(self):
        print('YetAnotherClass.method_a ' + self.method_b())
        if hasattr(self, 'df'):
            print('WORKS1')
        self.uncache('df')
        if not hasattr(self, 'df'):
            print('WORKS2')

    def method_b(self):
        return 'executed'


class Lane(object):
    def __init__(self, *args):
        self.tasks = []
        if len(args) > 0:
            for task in args:
                self.add(task)
        self.logger = get_logger(self.__class__.__name__)

    def __validate_task(self, cls, entry_mtd_name, args, kwargs):
        if not issubclass(cls, LaneTask):
            raise TypeError('Tried to add a non-Task class `%s` to a Lane. Are you sure you decorated it?'
                            % str(cls))
        print("IN _validate_task", cls.__init__)
        _validate_params(cls, entry_mtd_name, *args, **kwargs)

    def add(self, cls_or_branch, *args, **kwargs):
        if isinstance(cls_or_branch, Branch):
            return self

        # Validate
        self.__validate_task(cls_or_branch, '__init__', args, kwargs)

        # Append
        self.tasks.append({'cls': cls_or_branch, 'args': args, 'kwargs': kwargs})

        return self

    def run(self):
        for task_def in self.tasks:
            task = task_def['cls'](*task_def['args'], **task_def['kwargs'])
            task()


class Branch(Lane, object):
    def __init__(self, run_parallel=False):
        self.run_parallel = bool(run_parallel)
        super(Branch, self).__init__()

# print(ClsTest, type(ClsTest))
# print(dir(ClsTest))
# print(issubclass(ClsTest, LaneTask))
# print(ClsTest.__doc__)
# import inspect
# print(ClsTest.__init__, inspect.getargspec(ClsTest.__init__))
# print(Branch3Task2.__init__, inspect.getargspec(Branch3Task2.__init__))
# exit()
# task = ClsTest(1, 2)
# task()
# task = AnotherClassTest(1, 2)
# task()
# exit()

# _validate_params(ClsTest._base__init__, c=500, d=50000)
# exit()
# import inspect
# print(ClsTest, type(ClsTest))
# print('base', ClsTest.__bases__)
# print(ClsTest._base__init__)
# print(' INIT ARGSPEC ', inspect.getargspec(ClsTest._base__init__))
# # exit()
# print('instantiating')
# obj = ClsTest(500, 124124)
# print(obj, type(obj))
# obj()
# exit()

# print(ClsTest, ClsTest.__base__, isinstance(ClsTest, LaneTask), isinstance(ClsTest))
# lane = Lane()
# print(getattr(ClsTest, '__init__'), getattr(AnotherClassTest, '__init__'), getattr(YetAnotherClass, '__init__'))
# lane.add(ClsTest, c=500, d=50000).add(AnotherClassTest, 100, 200).add(YetAnotherClass, 1000, d=2000).run()
#
# exit()
# init_method = getattr(ClsTest, '_task_base__init__')
# _validate_params(ClsTest, '_task_base__init__', 1, 2)
# exit()
lane = Lane()
(lane
 .add(ClsTest, c=500, d=50000)
 .add(AnotherClassTest, 100, 200)
 .add(Branch()
      .add(Branch1Task)
      .add(Branch2Task1)
      .add(Branch2Task2)
      .add(Branch()
           .add(Branch2Task2SubTask1)
           .add(Branch2Task2SubTask2))
      .add(Branch3Task1)
      .add(Branch3Task2))
 .add(YetAnotherClass, 1000, d=2000))

from pprint import pprint
pprint(lane.tasks)
# lane.run()

# if __name__ == '__main__':
#     task = ClsTest('ccc', d='ddd')
#     print('LANE TASK?', isinstance(task, LaneTask))
#     print('TYPE', type(task))
#     print(dir(task))
#     task()
#     task = AnotherClassTest(1, 2)
#     task()
#     task = YetAnotherClass(1, 2)
#     try:
#         task()
#     except CacheError:
#         print('error was thrown!')
