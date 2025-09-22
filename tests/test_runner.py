"""
Test Runner for Tourism Database
===============================

Comprehensive test runner for the tourism database system.
Runs unit tests, integration tests, and regression tests.
"""

import unittest
import sys
import os
import argparse
import time
from typing import List, Optional

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TourismTestRunner:
    """Test runner for the tourism database system."""

    def __init__(self, verbosity: int = 2):
        """Initialize test runner."""
        self.verbosity = verbosity
        self.test_results = []

    def discover_tests(self, test_dir: str) -> unittest.TestSuite:
        """Discover tests in a directory."""
        loader = unittest.TestLoader()
        start_dir = os.path.join(os.path.dirname(__file__), test_dir)

        if not os.path.exists(start_dir):
            print(f"Warning: Test directory {start_dir} does not exist")
            return unittest.TestSuite()

        suite = loader.discover(start_dir, pattern='test_*.py')
        return suite

    def run_test_suite(self, suite: unittest.TestSuite, suite_name: str) -> unittest.TestResult:
        """Run a test suite and collect results."""
        print(f"\n{'='*60}")
        print(f"Running {suite_name} Tests")
        print(f"{'='*60}")

        runner = unittest.TextTestRunner(
            verbosity=self.verbosity,
            stream=sys.stdout,
            descriptions=True,
            failfast=False
        )

        start_time = time.time()
        result = runner.run(suite)
        end_time = time.time()

        # Store results
        self.test_results.append({
            'suite_name': suite_name,
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'skipped': len(result.skipped) if hasattr(result, 'skipped') else 0,
            'success': result.wasSuccessful(),
            'duration': end_time - start_time
        })

        return result

    def run_unit_tests(self) -> unittest.TestResult:
        """Run unit tests."""
        suite = self.discover_tests('unit')
        return self.run_test_suite(suite, 'Unit')

    def run_integration_tests(self) -> unittest.TestResult:
        """Run integration tests."""
        suite = self.discover_tests('integration')
        return self.run_test_suite(suite, 'Integration')

    def run_regression_tests(self) -> unittest.TestResult:
        """Run regression tests."""
        suite = self.discover_tests('regression')
        return self.run_test_suite(suite, 'Regression')

    def run_all_tests(self) -> List[unittest.TestResult]:
        """Run all test suites."""
        results = []

        print("Starting comprehensive test run for Tourism Database System")
        print(f"Python version: {sys.version}")
        print(f"Test runner verbosity: {self.verbosity}")

        # Run test suites in order of importance
        results.append(self.run_regression_tests())  # Most critical first
        results.append(self.run_unit_tests())
        results.append(self.run_integration_tests())

        return results

    def print_summary(self) -> None:
        """Print test execution summary."""
        print(f"\n{'='*60}")
        print("TEST EXECUTION SUMMARY")
        print(f"{'='*60}")

        total_tests = 0
        total_failures = 0
        total_errors = 0
        total_skipped = 0
        total_duration = 0
        all_success = True

        for result in self.test_results:
            print(f"\n{result['suite_name']} Tests:")
            print(f"  Tests run: {result['tests_run']}")
            print(f"  Failures: {result['failures']}")
            print(f"  Errors: {result['errors']}")
            print(f"  Skipped: {result['skipped']}")
            print(f"  Duration: {result['duration']:.2f}s")
            print(f"  Status: {'PASS' if result['success'] else 'FAIL'}")

            total_tests += result['tests_run']
            total_failures += result['failures']
            total_errors += result['errors']
            total_skipped += result['skipped']
            total_duration += result['duration']

            if not result['success']:
                all_success = False

        print(f"\n{'='*40}")
        print("OVERALL SUMMARY:")
        print(f"Total tests run: {total_tests}")
        print(f"Total failures: {total_failures}")
        print(f"Total errors: {total_errors}")
        print(f"Total skipped: {total_skipped}")
        print(f"Total duration: {total_duration:.2f}s")
        print(f"Overall status: {'PASS' if all_success else 'FAIL'}")

        if all_success:
            print("\n🎉 All tests passed! The system is ready for production.")
        else:
            print("\n❌ Some tests failed. Please review the failures before deploying.")

        return all_success

    def run_specific_test(self, test_path: str) -> unittest.TestResult:
        """Run a specific test file or test method."""
        print(f"Running specific test: {test_path}")

        loader = unittest.TestLoader()

        try:
            if '::' in test_path:
                # Specific test method: module::class::method
                module_path, test_name = test_path.split('::', 1)
                suite = loader.loadTestsFromName(test_name, __import__(module_path))
            else:
                # Test file or module
                suite = loader.loadTestsFromName(test_path)

            return self.run_test_suite(suite, 'Specific')

        except Exception as e:
            print(f"Error loading test {test_path}: {e}")
            return None


def main():
    """Main entry point for test runner."""
    parser = argparse.ArgumentParser(description='Run tourism database tests')
    parser.add_argument('--verbosity', '-v', type=int, default=2,
                       help='Test verbosity level (0-2)')
    parser.add_argument('--unit', action='store_true',
                       help='Run only unit tests')
    parser.add_argument('--integration', action='store_true',
                       help='Run only integration tests')
    parser.add_argument('--regression', action='store_true',
                       help='Run only regression tests')
    parser.add_argument('--test', type=str,
                       help='Run specific test (e.g., test_file.py or module::class::method)')
    parser.add_argument('--failfast', action='store_true',
                       help='Stop on first failure')

    args = parser.parse_args()

    runner = TourismTestRunner(verbosity=args.verbosity)

    if args.test:
        # Run specific test
        result = runner.run_specific_test(args.test)
        success = result.wasSuccessful() if result else False
    elif args.unit:
        # Run only unit tests
        result = runner.run_unit_tests()
        success = result.wasSuccessful()
    elif args.integration:
        # Run only integration tests
        result = runner.run_integration_tests()
        success = result.wasSuccessful()
    elif args.regression:
        # Run only regression tests
        result = runner.run_regression_tests()
        success = result.wasSuccessful()
    else:
        # Run all tests
        runner.run_all_tests()
        success = runner.print_summary()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()