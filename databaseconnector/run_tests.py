#!/usr/bin/env python3
"""
Test runner for the database connection module.

This script runs tests for the database connection module. It can run:
- Unit tests only (default)
- Integration tests only
- All tests
"""

import os
import sys
import argparse
import subprocess


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run tests for the database connection module")
    parser.add_argument(
        "--test-type",
        choices=["unit", "integration", "all"],
        default="unit",
        help="Type of tests to run (default: unit)"
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate code coverage report"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="count",
        default=0,
        help="Increase verbosity (can be used multiple times)"
    )
    return parser.parse_args()


def run_tests(test_type, coverage=False, verbose=0):
    """
    Run tests of the specified type.
    
    Args:
        test_type: Type of tests to run ("unit", "integration", or "all")
        coverage: Whether to generate coverage report
        verbose: Verbosity level (0-2)
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Set up command
    cmd = ["pytest"]
    
    # Add verbosity
    if verbose > 0:
        cmd.append("-" + "v" * min(verbose, 3))
    
    # Add coverage if requested
    if coverage:
        cmd += ["--cov=database", "--cov-report=term", "--cov-report=html"]
    
    # Add test path based on type
    if test_type == "unit":
        cmd.append("tests/unit/")
    elif test_type == "integration":
        cmd.append("tests/integration/")
    else:  # all
        cmd.append("tests/")
    
    # Print command
    print(f"Running: {' '.join(cmd)}")
    
    # Run the command
    return subprocess.call(cmd)


if __name__ == "__main__":
    args = parse_args()
    exit_code = run_tests(args.test_type, args.coverage, args.verbose)
    sys.exit(exit_code)