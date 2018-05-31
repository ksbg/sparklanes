from unittest import TestCase
from sparklanes.spark import set_context, set_session


class TestSpark(TestCase):
    def test_set_context_and_session(self):
        from sparklanes.spark import (context as sc, session as spark)

        # Test if imported instances work
        try:
            sc.range(3)
            spark.range(3)
        except Exception as e:
            self.fail('Failed to use imported SparkContext: %s' % str(e))

        # Check for default SparkContext name
        self.assertEqual(sc.appName, 'sparklanes')

        # Change the existing SparkContext.
        set_context(appName='test_change_context')
        from sparklanes.spark import context as sc

        # Check for new SparkContext name
        self.assertEqual(sc.appName, 'test_change_context')

        # Original Spark session should not work anymore
        try:
            spark.range(3)
        except Exception:
            pass

        # Change the Spark session
        set_session()
        from sparklanes.spark import session as spark

        # Both should now work again
        try:
            sc.range(3)
            spark.range(3)
        except Exception as e:
            self.fail('Failed to use newly created SparkContext: %s' % str(e))
