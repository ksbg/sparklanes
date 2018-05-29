import logging
import os
import sys
from unittest import TestCase

from six import PY2

from helpers.tasks import (UncacheAttribute, CacheAttribute, AccessCacheAttribute, ValidTask1, ValidTask2, ValidTask3,
                           ValidBranchTask1, ValidBranchTask2, ValidBranchTask3, ValidBranchTask2Subtask1,
                           ValidBranchTask2Subtask2, UndecoratedTask, ValidReqParams, ClearCache, AttributeDoesNotExist,
                           ExceptionThrowingTask)
from sparklanes.framework import utils
from sparklanes.framework.errors import (TaskInitializationError, LaneSchemaError, LaneImportError, LaneExecutionError,
                                         CacheError)
from sparklanes.framework.lane import Task, Lane, Branch, _LaneTask, _TaskCache

if PY2:
    from StringIO import StringIO
else:
    from io import StringIO


class TestUtils(TestCase):
    @classmethod
    def setUp(cls):
        super(TestUtils, cls).setUpClass()

        class StdOutCatcher(object):
            def __init__(self):
                self.data = []

            def write(self, s):
                self.data.append(s)

            def __str__(self):
                return "".join(self.data)

        cls.StdOutCatcher = StdOutCatcher

        # Capture logging
        cls.logger = utils.make_logger('sparklanes')
        cls.log_capture = StringIO()
        cls.logger.handlers[0].setLevel(logging.CRITICAL)
        ch = logging.StreamHandler(cls.log_capture)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(message)s'))  # Only capture message
        cls.logger.addHandler(ch)

        # Add tasks to path
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'helpers', 'tasks'))

        # Don't show print statements
        sys.stdout = open(os.devnull, 'w')

    def __call_param_validator(self, cls, mtd_name, should_raise, *args, **kwargs):
        if should_raise:
            try:
                self.assertRaises(TaskInitializationError, utils._validate_params, cls, mtd_name, *args, **kwargs)
            except AssertionError as e:
                print('Cls: `%s`, Mtd: `%s`, Args: `%s`, Kwargs: `%s`'
                      % (cls.__name__, mtd_name, str(args), str(kwargs)))
                raise e
        else:
            try:
                utils._validate_params(cls, mtd_name, *args, **kwargs)
            except Exception as e:
                self.fail('`%s.%s` raised unexpectedly with args `%s` and kwargs `%s`: \n%s.%s: %s'
                          % (cls.__name__, mtd_name, str(args), str(kwargs), e.__class__.__module__,
                             e.__class__.__name__, str(e)))

    def __cmp_lane_output(self, lane1, lane2):
        # Capture stdout
        lane1_catcher = self.StdOutCatcher()
        lane2_catcher = self.StdOutCatcher()

        # Catch lane1 output
        sys.stdout = lane1_catcher
        lane1.run()
        lane1_log_output = self.log_capture.getvalue()
        self.log_capture.truncate(0)

        # Catch lane2 output
        sys.stdout = lane2_catcher
        lane2.run()
        lane2_log_output = self.log_capture.getvalue()

        # Reset output
        self.log_capture.truncate(0)
        sys.stdout = sys.__stdout__

        # String to list (for easier debugging) & remove execution time from log
        lane1_log_output = lane1_log_output.split('\n')
        lane2_log_output = lane2_log_output.split('\n')
        for i in range(len(lane1_log_output)):
            pos = lane1_log_output[i].find('Execution time:')
            if pos != -1:
                lane1_log_output[i] = lane1_log_output[i][:pos]
        for i in range(len(lane2_log_output)):
            pos = lane2_log_output[i].find('Execution time:')
            if pos != -1:
                lane2_log_output[i] = lane2_log_output[i][:pos]

        # Compare
        self.assertEqual(lane1_catcher.data, lane2_catcher.data)
        self.assertEqual(lane1_log_output, lane2_log_output)

    def __get_abs_yml_path(self, name):
        return os.path.abspath(os.path.join(os.path.dirname(__file__), 'helpers', 'yml', name + '.yml'))

    def __fail(self, msg, e):
        self.fail(msg + '\n' + 'Exception: %s.%s: %s' % (e.__class__.__module__, e.__class__.__name__, str(e)))

    def test_invalid_lanes(self):
        # Lane empty
        lane = Lane()
        self.assertRaises(LaneExecutionError, lane.run)

        # Invalid name
        self.assertRaises(TypeError, Lane, self)

        # Trying to instantiate _LaneTask on its own
        self.assertRaises(TaskInitializationError, _LaneTask)

        # Trying to instantiate _TaskCache on its own
        self.assertRaises(CacheError, _TaskCache)


    def test_param_validation(self):
        class Cls(object):
            some_attribute = 100

            def __init__(self):
                pass

            @classmethod
            def cls_mtd_req(cls, a, b):
                pass

            @classmethod
            def cls_mtd_opt(cls, a=None, b=None, c=None):
                pass

            @classmethod
            def cls_mtd_reqopt(cls, a, b, c=None, d=None):
                pass

            @staticmethod
            def stc_mtd_req(a, b):
                pass

            @staticmethod
            def stc_mtd_opt(a=None, b=None, c=None):
                pass

            @staticmethod
            def stc_mtd_reqopt(a, b, c=None, d=None):
                pass

            def mtd_req(self, a, b):
                pass

            def mtd_opt(self, a=None, b=None, c=None):
                pass

            def mtd_reqopt(self, a, b, c=None, d=None):
                pass

        check = [
            ('__init__', [], {}, False),
            ('__init__', [1], {}, True),  # Too many args
            ('__init__', [], {'a': 1}, True),  # Too many kwargs
            ('__init__', [1], {'a': 1}, True),  # Too many args and kwargs
        ]

        for m in ('cls_mtd_req', 'stc_mtd_req', 'mtd_req'):
            check.append((m, [1, 2], {}, False))
            check.append((m, [], {'a': 1, 'b': 1}, False))
            check.append((m, [1], {'b': 1}, False))
            check.append((m, [], {}, True))  # No args
            check.append((m, [1], {'a': 1}, True))  # Duplicate args and kwargs
            check.append((m, [1, 2, 3], {}, True))  # Too many args
            check.append((m, [1], {'b': 1, 'c': 1}, True))  # Too many kwargs
            check.append((m, [1, 2, 3], {'a': 1, 'b': 1, 'c': 1}, True))  # Too many args and kwargs

        for m in ('cls_mtd_opt', 'stc_mtd_opt', 'mtd_opt'):
            check.append((m, [1, 2, 3], {}, False))
            check.append((m, [], {'a': 1, 'b': 1, 'c': 1}, False))
            check.append((m, [1], {'b': 1, 'c': 1}, False))
            check.append((m, [], {}, False))
            check.append((m, [1, 2], {'c': 1}, False))
            check.append((m, [1, 2, 3], {'b': 1}, True))  # Duplicate args and kwargs (missed required arg)
            check.append((m, [1, 2, 3, 4, 5], {}, True))  # Too many args
            check.append((m, [1], {'b': 1, 'c': 1, 'd': 1}, True))  # Too many kwargs
            check.append((m, [1, 2, 3], {'a': 1, 'b': 1, 'c': 1, 'd': 1}, True))  # Too many args and kwargs

        for m in ('cls_mtd_reqopt', 'stc_mtd_reqopt', 'mtd_reqopt'):
            check.append((m, [1, 2], {}, False))
            check.append((m, [1, 2, 3, 4], {}, False))
            check.append((m, [1], {'b': 1, 'c': 1, 'd': 1}, False))
            check.append((m, [], {'a': 1, 'b': 1, 'c': 1, 'd': 1}, False))
            check.append((m, [], {}, True))  # No args
            check.append((m, [1, 2, 3], {'b': 1}, True))  # Duplicate args and kwargs
            check.append((m, [1], {'c': 1, 'd': 1}, True))  # Missed required arg
            check.append((m, [1], {'b': 1, 'c': 1, 'd': 1, 'e': 1}, True))  # Too many kwargs
            check.append((m, [1, 2, 3], {'c': 1, 'd': 1}, True))  # Too many args and kwargs

        for m, a, k, s in check:
            self.__call_param_validator(Cls, m, s, *a, **k)

        # Not a method
        self.assertRaises(TypeError, utils._validate_params, Cls, 'some_attribute')

        # Not an attribute
        self.assertRaises(AttributeError, utils._validate_params, Cls, 'not_an_attribute')

    def test_valid_yaml_lanes(self):
        lane = (Lane(name='ValidSimple')
                .add(ValidReqParams, 1, 2, c=3)
                .add(ValidTask1)
                .add(ValidTask2)
                .add(ValidTask3))
        lane_from_yaml = utils.build_lane_from_yaml(self.__get_abs_yml_path('valid_simple'))
        self.__cmp_lane_output(lane, lane_from_yaml)

        lane = (Lane(name='ValidBranched')
                .add(ValidTask1)
                .add(ValidTask2)
                .add(Branch(name='ValidBranch')
                     .add(ValidBranchTask1)
                     .add(ValidBranchTask2)
                     .add(ValidBranchTask3))
                .add(ValidTask3))
        lane_from_yaml = utils.build_lane_from_yaml(self.__get_abs_yml_path('valid_branched'))
        self.__cmp_lane_output(lane, lane_from_yaml)

        lane = (Lane(name='ValidBranchedNested')
                .add(ValidTask1)
                .add(ValidTask2)
                .add(Branch(name='ValidBranch')
                     .add(ValidBranchTask1)
                     .add(ValidBranchTask2)
                     .add(Branch(name='ValidSubBranch')
                          .add(ValidBranchTask2Subtask1)
                          .add(ValidBranchTask2Subtask2))
                     .add(ValidBranchTask3))
                .add(ValidTask3))
        lane_from_yaml = utils.build_lane_from_yaml(self.__get_abs_yml_path('valid_branched_nested'))
        self.__cmp_lane_output(lane, lane_from_yaml)

        lane = (Lane(name='ValidBranchedNestedDeeply')
                .add(ValidTask1)
                .add(ValidTask2)
                .add(Branch(name='ValidBranch')
                     .add(ValidBranchTask1)
                     .add(ValidBranchTask2)
                     .add(Branch(name='ValidSubBranch')
                          .add(ValidBranchTask2Subtask1)
                          .add(Branch(name='ValidSubSubBranch')
                               .add(ValidTask1)
                               .add(Branch(name='ValidSubSubSubBranch')
                                    .add(ValidTask2)
                                    .add(ValidTask2))
                               .add(ValidTask3))
                          .add(ValidBranchTask2Subtask2))
                     .add(ValidBranchTask3))
                .add(ValidTask3))
        lane_from_yaml = utils.build_lane_from_yaml(self.__get_abs_yml_path('valid_branched_nested_deeply'))
        self.__cmp_lane_output(lane, lane_from_yaml)

    def test_invalid_yaml_lanes(self):
        self.assertRaises(LaneImportError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_nonexistant_class'))
        self.assertRaises(LaneImportError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_nonexistant_module'))
        self.assertRaises(LaneImportError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_class_without_module'))
        self.assertRaises(LaneSchemaError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_schema_missing_fields'))
        self.assertRaises(LaneSchemaError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_schema_redundant_fields'))
        self.assertRaises(LaneSchemaError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_schema_wrong_types'))
        self.assertRaises(TaskInitializationError,
                          utils.build_lane_from_yaml,
                          self.__get_abs_yml_path('invalid_too_many_params'))

    def test_caching(self):

        # Should not throw
        lane = (Lane()
                .add(CacheAttribute, 'a')
                .add(AccessCacheAttribute, 'a')
                .add(AttributeDoesNotExist, 'b')
                .add(CacheAttribute, 'b')
                .add(AccessCacheAttribute, 'b')
                .add(UncacheAttribute, 'b')
                .add(AttributeDoesNotExist, 'b')
                .add(AccessCacheAttribute, 'a')
                .add(CacheAttribute, 'c')
                .add(ClearCache)
                .add(AttributeDoesNotExist, 'a')
                .add(AttributeDoesNotExist, 'b')
                .add(AttributeDoesNotExist, 'c')
                .add(CacheAttribute, 'd')
                .add(AccessCacheAttribute, 'd')
                .add(UncacheAttribute, 'd')
                .add(AttributeDoesNotExist, 'd')
                .add(ClearCache))

        try:
            lane.run()
        except Exception as e:
            self.__fail('Lane execution should not have failed for the following lane: \n %s' % str(lane), e)

        # Access of non-cached attribute
        lane = Lane().add(AccessCacheAttribute, 'a')
        self.assertRaises(AttributeError, lane.run)

        # Access after clearing attributes
        lane = Lane().add(CacheAttribute, 'a').add(CacheAttribute, 'b').add(ClearCache).add(AccessCacheAttribute, 'b')
        self.assertRaises(AttributeError, lane.run)

        # Access after uncaching added attribute
        lane = Lane().add(CacheAttribute, 'a').add(UncacheAttribute, 'a').add(AccessCacheAttribute, 'a')
        self.assertRaises(AttributeError, lane.run)

    def test_exception_catching(self):
        lane = Lane().add(ValidTask1).add(ExceptionThrowingTask).add(ValidTask2)
        self.assertRaises(Exception, lane.run)

        # From Threads
        lane = Lane(run_parallel=True).add(ValidTask1).add(ExceptionThrowingTask).add(ValidTask2)
        self.assertRaises(Exception, lane.run)

    def test_class_decoration(self):
        def make_decorated_without_method_task():
            @Task
            class DecoratedWithoutMethod(object):
                def mtd(self):
                    print('DecoratedWithoutMethod executed')

            return DecoratedWithoutMethod

        def make_decorated_with_invalid_method_task():
            @Task('mtdd')
            class DecoratedWithInvalidMethod(object):
                def mtd(self):
                    print('DecoratedWithInvalidMethod executed')

            return DecoratedWithInvalidMethod

        self.assertRaises(TypeError, make_decorated_without_method_task)
        self.assertRaises(TypeError, make_decorated_with_invalid_method_task)

        # Try adding undecorated class to a lane
        self.assertRaises(TypeError, Lane().add, UndecoratedTask)

        # Check decorated class type
        self.assertEqual(issubclass(ValidTask1, _LaneTask), True)
        self.assertEqual(issubclass(UndecoratedTask, _LaneTask), False)

        # Check if decorated classes have expected attributes
        for attr in ('_entry_mtd', 'cache', 'uncache', 'clear_cache', '__call__'):
            if not hasattr(ValidTask1, attr):
                raise AssertionError('Decorated class `ValidTask1` is supposed to have an attribute `%s`' % attr)
        self.assertEqual(ValidTask1.__name__, 'Task_ValidTask1')

    @classmethod
    def tearDownClass(cls):
        sys.stdout = sys.__stdout__
