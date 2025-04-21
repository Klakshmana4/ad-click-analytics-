import unittest
from ads_analytics import CTRCalculator
from pyflink.common import Time
from pyflink.datastream.window import TimeWindow

class TestCTRCalculator(unittest.TestCase):
    def test_ctr_calculation(self):
        calculator = CTRCalculator()
        key = "camp-1"
        context = MockContext()  # Mock the window context
        elements = [(
            "imp-1", "user-1", "camp-1", "ad-1", "mobile", "chrome", 1647890123456, 0.01
        ),(
            "imp-2", "user-2", "camp-1", "ad-2", "desktop", "firefox", 1647890123457, 0.02
        ),(
            "imp-3", "user-1", "camp-1", "ad-3", "mobile", "safari", 1647890128456, 0.03
        )]
        out = []
        calculator.process(key, context, elements, out)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0][0], "camp-1")
        self.assertEqual(out[0][1], 0.0)  

class MockContext:  # Dummy context object for testing
    class MockWindow:
        def end(self):
            return 1647890183456  # Example timestamp

    def window(self):
        return self.MockWindow()

if __name__ == '__main__':
    unittest.main()
