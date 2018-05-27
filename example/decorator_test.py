from inspect import isclass
from six import PY3, string_types

from sparklanes import Task
from tabulate import tabulate
from sparklanes.framework.task_core import LaneTask
from sparklanes.framework.utils import _validate_params
from sparklanes.spark import get_logger
from sparklanes.framework.errors import LaneExecutionError
import sys
import threading

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
class Branch2Task2SubTask2(object):
    def mtd(self):
        # raise TypeError('THIS IS A TEST EXCEPTION')
        print('Branch2Task2SubTask2 executed')


@Task('mtd')
class Branch2Task2SubTask1(object):
    def mtd(self):
        from time import sleep
        print('SLEEPING............................................................')
        sleep(1)
        print('Branch2Task2SubTask1 executed')


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


lane = Lane(name='JustATestLane')
(lane
 .add(ClsTest, c=500, d=50000)
 .add(AnotherClassTest, 100, 200)
 .add(Branch(name='JustATestBranch', run_parallel=True)
      .add(Branch1Task)
      .add(Branch2Task1)
      .add(Branch2Task2)
      .add(Branch(name='InnerBranch', run_parallel=True)
           .add(Branch2Task2SubTask1)
           .add(Branch2Task2SubTask2))
      .add(Branch3Task1)
      .add(Branch3Task2))
 .add(YetAnotherClass, 1000, d=2000))

print(str(lane))

lane.run()

