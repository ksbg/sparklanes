from unittest import TestSuite, TextTestRunner, makeSuite

from .test_lane import TestLane
from .test_spark import TestSpark
from .test_submit import TestSparkSubmit

suite = TestSuite()
suite.addTest(makeSuite(TestLane))
suite.addTest(makeSuite(TestSparkSubmit))
suite.addTest(makeSuite(TestSpark))
runner = TextTestRunner(verbosity=2)
runner.run(suite)
