import os
import sys
from unittest import TestCase

from sparklanes import conn, Lane
from sparklanes._framework.env import SPARK_APP_NAME, VERBOSE_TESTING
from .helpers.tasks import ChangeContext, UseContext


class TestSpark(TestCase):
    def setUp(self):
        # Make sure we're using the default context
        conn.sc.stop()
        conn.init_default()

    def test_set_context_and_session(self):
        # Test if imported instances work
        try:
            conn.sc.range(3)
            conn.spark.range(3)
        except Exception as exc:
            self.fail('Failed to use imported SparkContext: %s' % str(exc))

        # Check for default SparkContext name
        self.assertEqual(conn.sc.appName, SPARK_APP_NAME)

        # Change the existing SparkContext and store the old one
        old_sc = conn.sc
        conn.set_sc(appName='test_change_context')

        # Check for new SparkContext name
        self.assertEqual(conn.sc.appName, 'test_change_context')

        # Old SparkContext should not work anymore
        self.assertRaises(AttributeError, old_sc.range, 3)

        # Change the session
        conn.set_spark()

        # Both should now work again
        try:
            conn.sc.range(3)
            conn.spark.range(3)
        except Exception as exc:
            self.fail('Failed to use newly created SparkContext: %s' % str(exc))

    def test_context_change_during_tasks(self):
        with open(os.devnull, 'w') as devnull:
            if not VERBOSE_TESTING:
                sys.stdout = devnull
                sys.stderr = devnull

            lane = (Lane(name='test_context_change_during_tasks')
                    .add(UseContext)
                    .add(ChangeContext)
                    .add(UseContext)
                    .add(ChangeContext)
                    .add(UseContext))

            try:
                lane.run()
            except Exception as exc:
                self.fail('Failed to run valid lane: %s' % str(exc))
