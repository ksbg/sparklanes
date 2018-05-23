import warnings
from unittest import TestCase
from uuid import uuid4

from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from six import PY3

from pysparketl.etl import errors
from pysparketl.etl.shared import Shared


class TestShared(TestCase):
    def setUp(self):
        if PY3:
            warnings.simplefilter('ignore', ResourceWarning)
        self.sc = SparkContext.getOrCreate()
        self.spark = SparkSession.Builder().appName('TestShared').getOrCreate()
        self.a_resource = [('first', 1), ('second', 2), ('third', 3)]
        self.a_rdd = self.sc.parallelize(self.a_resource).map(lambda x: Row(key=x[0], number=int(x[1])))
        self.a_data_frame = self.spark.createDataFrame(self.a_rdd)

        self.invalid_names = [1, Row, [], None, {'a': 1}]
        self.valid_names = ['name', u'name']
        self.rdd_df_invalid = self.invalid_names + self.valid_names

    def __test_names(self, mtd, valid_arg=None):
        for n in self.invalid_names:
            args = [n, valid_arg] if valid_arg else [n]
            self.assertRaises(errors.PipelineInvalidResourceNameError, mtd, *args)
        try:
            for n in self.valid_names:
                args = [n, valid_arg] if valid_arg else [n]
                mtd(*args)
        except errors.PipelineInvalidResourceNameError as e:
            self.fail('\nException raised for valid name: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))
        except (errors.PipelineSharedResourceNotFound, errors.PipelineSharedResourceError):
            pass

    def test_add_resource(self):
        self.__test_names(mtd=Shared.add_resource, valid_arg=self.a_resource)
        try:
            n = str(uuid4())
            Shared.add_resource(name=n, res=[1, 2, 3])
        except Exception as e:
            self.fail('\nException raised for valid resource: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Check if added resource was saved
        try:
            Shared.get_resource(n)
        except Exception as e:
            self.fail('\nException raised when retrieving resource expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_add_data_frame(self):
        self.__test_names(mtd=Shared.add_data_frame, valid_arg=self.a_data_frame)
        # Add valid DataFrame
        try:
            n = str(uuid4())
            Shared.add_data_frame(name=n, df=self.a_data_frame)
        except Exception as e:
            self.fail('\nException raised for valid DataFrame: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Check if added DataFrame was saved
        try:
            Shared.get_data_frame(n)
        except Exception as e:
            self.fail('\nException raised when retrieving DataFrame expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Add invalid DataFrame
        for invld in self.rdd_df_invalid:
            self.assertRaises(errors.PipelineSharedResourceTypeInvalid, Shared.add_data_frame, str(uuid4()), invld)

    def test_add_rdd(self):
        self.__test_names(mtd=Shared.add_rdd, valid_arg=self.a_rdd)
        # Add valid RDD
        try:
            n = str(uuid4())
            Shared.add_rdd(name=n, rdd=self.a_rdd)
        except Exception as e:
            self.fail('\nException raised for valid RDD: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Check if added RDD was saved
        try:
            Shared.get_rdd(n)
        except Exception as e:
            self.fail('\nException raised when retrieving RDD expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Add invalid RDD
        for invld in self.rdd_df_invalid:
            self.assertRaises(errors.PipelineSharedResourceTypeInvalid, Shared.add_rdd, str(uuid4()), invld)

    def test_delete_resource(self):
        self.__test_names(mtd=Shared.add_resource, valid_arg=self.a_resource)

        # Delete non-existing resource
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.delete_resource, str(uuid4()))

        # Add resource
        n = str(uuid4())
        Shared.add_resource(name=n, res=self.a_resource)

        # Delete added resource
        try:
            Shared.delete_resource(name=n)
        except Exception as e:
            self.fail('\nException raised when trying to delete resource expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_delete_data_frame(self):
        self.__test_names(mtd=Shared.add_data_frame, valid_arg=self.a_data_frame)

        # Delete non-existing DataFrame
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.delete_data_frame, str(uuid4()))

        # Add DataFrame
        n = str(uuid4())
        Shared.add_data_frame(name=n, df=self.a_data_frame)

        # Delete added DataFrame
        try:
            Shared.delete_data_frame(name=n)
        except Exception as e:
            self.fail('\nException raised when trying to delete DataFrame expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_delete_rdd(self):
        self.__test_names(mtd=Shared.add_rdd, valid_arg=self.a_rdd)

        # Delete non-existing resource
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.delete_rdd, str(uuid4()))

        # Add RDD
        n = str(uuid4())
        Shared.add_rdd(name=n, rdd=self.a_rdd)

        # Delete added resource
        try:
            Shared.delete_rdd(name=n)
        except Exception as e:
            self.fail('\nException raised when trying to delete RDD expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_update_resource(self):
        self.__test_names(mtd=Shared.add_resource, valid_arg=self.a_resource)

        # Update non-existing resource
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.update_resource, str(uuid4()), self.a_resource)

        # Add resource
        n = str(uuid4())
        Shared.add_resource(name=n, res=self.a_resource)

        # Update added resource
        try:
            Shared.update_resource(name=n, res=self.a_resource)
        except Exception as e:
            self.fail('\nException raised when trying to update resource expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_update_data_frame(self):
        self.__test_names(mtd=Shared.add_data_frame, valid_arg=self.a_data_frame)

        # Update non-existing DataFrame
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.update_data_frame, str(uuid4()),
                          self.a_data_frame)

        # Add DataFrame
        n = str(uuid4())
        Shared.add_data_frame(name=n, df=self.a_data_frame)

        # Update added DataFrame
        try:
            Shared.update_data_frame(name=n, df=self.a_data_frame)
        except Exception as e:
            self.fail('\nException raised when trying to update DataFrame expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Update with invalid DataFrame
        for invld in self.rdd_df_invalid:
            self.assertRaises(errors.PipelineSharedResourceTypeInvalid, Shared.update_data_frame, n, invld)

    def test_update_rdd(self):
        self.__test_names(mtd=Shared.add_rdd, valid_arg=self.a_rdd)

        # Update non-existing RDD
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.update_rdd, str(uuid4()),
                          self.a_rdd)

        # Add RDD
        n = str(uuid4())
        Shared.add_rdd(name=n, rdd=self.a_rdd)

        # Update added RDD
        try:
            Shared.update_rdd(name=n, rdd=self.a_rdd)
        except Exception as e:
            self.fail('\nException raised when trying to update RDD expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

        # Update with invalid RDD
        for invld in self.rdd_df_invalid:
            self.assertRaises(errors.PipelineSharedResourceTypeInvalid, Shared.update_rdd, n, invld)

    def test_get_resource(self):
        self.__test_names(mtd=Shared.get_resource)

        # Get non-existing resource
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.get_resource, str(uuid4()))

        # Add resource
        n = str(uuid4())
        Shared.add_resource(name=n, res=self.a_resource)

        # Get added resource
        try:
            Shared.get_resource(n)
        except Exception as e:
            self.fail('\nException raised when trying to get resource expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_get_data_frame(self):
        self.__test_names(mtd=Shared.get_data_frame)

        # Get non-existing DataFrame
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.get_data_frame, str(uuid4()))

        # Add DataFrame
        n = str(uuid4())
        Shared.add_data_frame(name=n, df=self.a_data_frame)

        # Get added DataFrame
        try:
            Shared.get_data_frame(n)
        except Exception as e:
            self.fail('\nException raised when trying to get DataFrame expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_get_rdd(self):
        self.__test_names(mtd=Shared.get_rdd)

        # Get non-existing RDD
        self.assertRaises(errors.PipelineSharedResourceNotFound, Shared.get_rdd, str(uuid4()))

        # Add DataFrame
        n = str(uuid4())
        Shared.add_rdd(name=n, rdd=self.a_rdd)

        # Get added DataFrame
        try:
            Shared.get_rdd(n)
        except Exception as e:
            self.fail('\nException raised when trying to get RDD expected to be existing: %s\nMessage: %s'
                      % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e)))

    def test_get_all_resources(self):
        # Delete all first
        Shared.delete_all()

        # Add some
        ress = ([1, 2, 3],
                [4, 5, 6],
                [7, 8, 9])
        for n, res in zip(('a', 'b', 'c'), ress):
            Shared.add_resource(n, res)

        # Get all and check if equal to added ones
        all_resources = Shared.get_all_resources()
        self.assertEqual(len(all_resources), len(ress))
        for n, res, retrvd_n, retrvd_res in zip(('a', 'b', 'c'), ress, all_resources.keys(), all_resources.values()):
            self.assertEqual(n, retrvd_n)
            self.assertEqual(res, retrvd_res)

    def test_get_all_data_frames(self):
        # Delete all first
        Shared.delete_all()

        # Add some
        data = ((('1', 1), ('1', 1)),
                (('3', 3), ('5', 5)),
                (('6', 6), ('7', 7)))
        rdds = [self.sc.parallelize(d).map(lambda x: Row(key=x[0], number=int(x[1]))) for d in data]
        for n, rdd in zip(('a', 'b', 'c'), rdds):
            Shared.add_rdd(n, rdd)

        # Get all and check if equal to added ones
        all_rdds = Shared.get_all_rdds()
        self.assertEqual(len(all_rdds), len(rdds))
        for n, rdd, retrvd_n, retrvd_rdd in zip(('a', 'b', 'c'), rdds, all_rdds.keys(), all_rdds.values()):
            self.assertEqual(n, retrvd_n)
            self.assertEqual(rdd, retrvd_rdd)

    def test_get_all_rdds(self):
        # Delete all first
        Shared.delete_all()

        # Add some
        data = ((('1', 1), ('1', 1)),
                (('3', 3), ('5', 5)),
                (('6', 6), ('7', 7)))
        rdds = [self.sc.parallelize(d).map(lambda x: Row(key=x[0], number=int(x[1]))) for d in data]
        dfs = [self.spark.createDataFrame(rdd) for rdd in rdds]
        for n, df in zip(('a', 'b', 'c'), dfs):
            Shared.add_data_frame(n, df)

        # Get all and check if equal to added ones
        all_dfs = Shared.get_all_data_frames()
        self.assertEqual(len(all_dfs), len(dfs))
        for n, df, retrvd_n, retrvd_df in zip(('a', 'b', 'c'), dfs, all_dfs.keys(), all_dfs.values()):
            self.assertEqual(n, retrvd_n)
            self.assertEqual(df, retrvd_df)

    def test_delete_all(self):
        # Add some resources
        ress = ([1, 2, 3],
                [4, 5, 6],
                [7, 8, 9])
        for n, res in zip(('a', 'b', 'c'), ress):
            Shared.add_resource(n, res)

        # Add some RDDs
        data = ((('1', 1), ('1', 1)),
                (('3', 3), ('5', 5)),
                (('6', 6), ('7', 7)))
        rdds = [self.sc.parallelize(d).map(lambda x: Row(key=x[0], number=int(x[1]))) for d in data]
        for n, rdd in zip(('a', 'b', 'c'), rdds):
            Shared.add_rdd(n, rdd)

        # Add some DataFrames
        dfs = [self.spark.createDataFrame(rdd) for rdd in rdds]
        for n, df in zip(('a', 'b', 'c'), dfs):
            Shared.add_data_frame(n, df)

        # Check if empty
        for all_r in [Shared.get_all_resources(), Shared.get_all_data_frames(), Shared.get_all_rdds()]:
            self.assertNotEqual(len(all_r), 0)

        # Now delete all
        Shared.delete_all()

        for all_r in [Shared.get_all_resources(), Shared.get_all_data_frames(), Shared.get_all_rdds()]:
            self.assertEqual(len(all_r), 0)

    def test_exists(self):
        self.assertEqual(Shared.resource_exists('a'), False)
        Shared.add_resource('a', [1, 2, 3])
        self.assertEqual(Shared.resource_exists('a'), True)

        self.assertEqual(Shared.rdd_exists('b'), False)
        rdd = self.sc.parallelize((('1', 1), ('1', 1))).map(lambda x: Row(key=x[0], number=int(x[1])))
        Shared.add_rdd('b', rdd)
        self.assertEqual(Shared.rdd_exists('b'), True)

        self.assertEqual(Shared.df_exists('c'), False)
        df = self.spark.createDataFrame(rdd)
        Shared.add_data_frame('c', df)
        self.assertEqual(Shared.df_exists('c'), True)

    def tearDown(self):
        Shared.delete_all()
