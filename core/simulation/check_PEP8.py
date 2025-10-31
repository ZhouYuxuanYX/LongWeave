#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Checks a given Python file for style violations, programming errors,
and code complexity issues using the flake8 library and common plugins.

This provides stricter checks than basic pycodestyle (PEP 8). It includes:
- PEP 8 Style checks (pycodestyle, E/W codes)
- Logical Error checks (pyflakes, F codes)
- Potential Bug checks (flake8-bugbear, B codes)
- Comprehension Simplifications (flake8-comprehensions, C codes)
- Code Simplification checks (flake8-simplify, SIM codes)
- McCabe Complexity checks (flake8 default, C90 codes - often disabled by default)

Reports the total number of violations and lists each specific violation
with its line number, column number, error code, and description.
"""

import argparse
import os
import sys
# We now use flake8's API instead of pycodestyle
from flake8.api import legacy as flake8
from flake8.formatting import base

# --- Configuration for Checks ---
# Select codes to check. This list represents the "higher requirements".
# E/W: PEP 8 Style (from pycodestyle)
# F: Logical errors (from pyflakes)
# B: Potential bugs/dubious code (from flake8-bugbear) - REQUIRES PLUGIN
# C4: Comprehension simplifications (from flake8-comprehensions) - REQUIRES PLUGIN
# SIM: Code simplifications (from flake8-simplify) - REQUIRES PLUGIN
# C90: McCabe complexity (built-in but often disabled) - We'll keep it optional/maybe disable
# Note: Ensure the required plugins are installed (`pip install flake8 flake8-bugbear flake8-comprehensions flake8-simplify`)
CODES_TO_CHECK = ['E', 'W', 'F', 'B', 'C4', 'SIM']
# You might want to ignore specific codes globally here if needed
CODES_TO_IGNORE = ['C901'] # Example: Ignore complexity warnings by default

# --- Flake8 Custom Reporter ---

class ViolationCollector(base.BaseFormatter):
    """
    A custom flake8 formatter that collects violation details.
    It mimics the structure of the original pycodestyle reporter.
    """
    def __init__(self, options):
        super().__init__(options)
        self._errors = []

    def start(self):
        """Called before any files are processed."""
        self._errors = [] # Ensure list is fresh for each run

    def handle(self, error):
        """Collect error details from a flake8 Error object."""
        self._errors.append({
            'line': error.line_number,
            'col': error.column_number, # flake8 provides 1-based column
            'code': error.code,
            'message': error.text # The message part of "CODE message"
        })

    # No 'stop' method needed unless cleanup is required

    @property
    def violations(self) -> list:
        """Return the list of collected violation dictionaries."""
        # Sort violations by line number, then column for consistent output
        return sorted(self._errors, key=lambda v: (v['line'], v['col']))

    @property
    def count(self) -> int:
        """Return the total number of collected violations."""
        return len(self._errors)

# --- Checking Function ---

def run_flake8_check(filename: str) -> tuple[int, list]:
    """
    Checks a file using flake8 with selected plugins.

    Args:
        filename: The path to the Python file to check.

    Returns:
        A tuple containing:
            - The total number of violations found (int).
            - A list of violation dictionaries (list). Each dict contains
              'line', 'col', 'code', and 'message'.
        Returns (-1, []) on file error.
    """
    if not os.path.exists(filename):
        print(f"Error: File not found: {filename}", file=sys.stderr)
        return -1, [] # Indicate error

    if not os.path.isfile(filename):
        print(f"Error: Not a file: {filename}", file=sys.stderr)
        return -1, []

    print(f"--- Running flake8 check on: {filename} ---")
    print(f"--- Checking codes: {', '.join(CODES_TO_CHECK)} "
          f"(Ignoring: {', '.join(CODES_TO_IGNORE)}) ---")
    print("--- Requires plugins: flake8-bugbear, flake8-comprehensions, flake8-simplify ---")

    # Configure flake8 to use our custom reporter and selected checks
    # We pass the *class* to get_style_guide, it handles instantiation.
    style_guide = flake8.get_style_guide(
        select=CODES_TO_CHECK,
        ignore=CODES_TO_IGNORE,
        # Use our custom class for formatting/reporting
        # format='default', # Setting format might override reporter
        # reporter=ViolationCollector # This seems to be the correct way in newer APIs
        # Instead of reporter, we might need to pass the class via `report_class`
        # Let's try passing the class directly to the run method if possible,
        # or configure via a formatter entry point if needed.
        # Update: The API seems to expect the class directly for the 'formatter' key
        # *when used programmatically*.
        formatter=ViolationCollector, # Pass the class here
        max_line_length=pycodestyle.MAX_LINE_LENGTH # Use pycodestyle's constant if needed
    )

    # The check_files method returns the Report object (our collector instance)
    report = style_guide.check_files([filename])

    # Access the collected violations and count from our reporter instance
    # The instance used is the one returned by check_files
    return report.count, report.violations


# --- Main Execution ---

def main():
    """Main function to parse arguments and run the enhanced check."""
    parser = argparse.ArgumentParser(
        description="Check a Python file using flake8 with enhanced rules "
                    "(style, errors, complexity, best practices via plugins).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "filename",
        help="Path to the Python file to check."
    )
    # Could add arguments here to customize --select or --ignore if desired
    args = parser.parse_args()

    total_violations, violation_details = run_flake8_check(args.filename)

    if total_violations == -1:
        sys.exit(1) # Exit with error if file check failed

    if violation_details:
        print("\nSpecific Violations Found:")
        for violation in violation_details:
            print(f"  Line {violation['line']:<4} Col {violation['col']:<3}: "
                  f"[{violation['code']:<5}] {violation['message']}") # Wider code field
    elif total_violations == 0:
        print("\nNo violations found according to selected flake8 rules. Excellent!")

    print(f"\nTotal violations found: {total_violations}")

    print("\n--- Check Complete ---")


if __name__ == "__main__":
    # Important: Ensure required plugins are installed in the environment
    # where this script runs, otherwise B, C4, SIM codes won't be found.
    # Example: pip install flake8 flake8-bugbear flake8-comprehensions flake8-simplify
    try:
        # Optional: Check if flake8 is available early
        import flake8
        import flake8_bugbear # Check for plugin presence
        import flake8_comprehensions
        import flake8_simplify
    except ImportError as e:
        print(f"Error: Missing required library or plugin: {e}", file=sys.stderr)
        print("Please install required packages: "
              "pip install flake8 flake8-bugbear flake8-comprehensions flake8-simplify",
              file=sys.stderr)
        sys.exit(1)

    main()