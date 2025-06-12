"""
SQLAlchemy 1.3 Migration Detector Monkey Patch

This monkey patch detects patterns in SQLAlchemy 1.3 that will break in 1.4:
1. Row objects used in comparisons
2. Query objects used in in_() clauses
3. Implicit conversions that will fail in 1.4

Usage:
    Import this module BEFORE any SQLAlchemy imports in your application:

    import sqlalchemy_migration_detector
    import sqlalchemy
    # ... rest of your code

The patch will log warnings whenever it detects problematic patterns.
"""

import logging
import sys
import traceback
import warnings
from functools import wraps


# Set up logging
logger = logging.getLogger("sqlalchemy_migration_detector")
logger.setLevel(logging.WARNING)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[MIGRATION WARNING] %(message)s\nLocation: %(pathname)s:%(lineno)d")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_caller_info():
    """Get information about where the problematic code is being called from"""
    frame = sys._getframe()
    # Walk up the stack to find the first frame outside this module
    while frame:
        filename = frame.f_code.co_filename
        if "sqlalchemy" not in filename:
            return {"filename": filename, "lineno": frame.f_lineno, "function": frame.f_code.co_name}
        frame = frame.f_back
    return {"filename": "unknown", "lineno": 0, "function": "unknown"}


def warn_migration_issue(message, category="GENERAL"):
    """Log a migration warning with caller information"""
    caller = get_caller_info()
    full_message = f"[{category}] {message}"

    # Create a LogRecord with the caller's location
    record = logging.LogRecord(
        name=logger.name,
        level=logging.WARNING,
        pathname=caller["filename"],
        lineno=caller["lineno"],
        msg=full_message,
        args=(),
        exc_info=None,
    )
    logger.handle(record)


def patch_row_comparisons():
    """Patch Row objects to detect when they're used in comparisons"""
    try:
        from sqlalchemy.engine import Row
        from sqlalchemy.engine.result import RowProxy  # SQLAlchemy 1.3

        # Store original methods
        original_row_eq = Row.__eq__ if hasattr(Row, "__eq__") else None
        original_rowproxy_eq = RowProxy.__eq__ if hasattr(RowProxy, "__eq__") else None

        def patched_row_comparison(original_method, obj_type):
            @wraps(original_method)
            def wrapper(self, other):
                # Check if we're comparing with a column or in a filter context
                from sqlalchemy.sql.elements import ColumnElement

                if isinstance(other, ColumnElement):
                    warn_migration_issue(
                        f"{obj_type} object being compared to Column. "
                        f"In SQLAlchemy 1.4, you'll need to extract the scalar value: "
                        f"use .scalar() or row[0] instead of the full Row object.",
                        "ROW_COMPARISON",
                    )

                return original_method(self, other)

            return wrapper

        # Patch Row.__eq__ if it exists
        if original_row_eq:
            Row.__eq__ = patched_row_comparison(original_row_eq, "Row")

        # Patch RowProxy.__eq__ if it exists (SQLAlchemy 1.3)
        if original_rowproxy_eq:
            RowProxy.__eq__ = patched_row_comparison(original_rowproxy_eq, "RowProxy")

    except ImportError:
        # Row/RowProxy might not be available in all versions
        pass


def patch_query_in_operations():
    """Patch Query objects to detect when they're used in in_() operations"""
    try:
        from sqlalchemy.orm.query import Query
        from sqlalchemy.sql.operators import in_op

        # Store original Query.__iter__ to detect when Query is being evaluated
        original_query_iter = Query.__iter__

        @wraps(original_query_iter)
        def patched_query_iter(self):
            # Get the current stack to see if we're in an in_() operation
            stack = traceback.extract_stack()

            # Look for signs we're being used in a comparison context
            for frame in reversed(stack[-10:]):  # Check last 10 frames
                if any(
                    keyword in frame.line.lower()
                    for keyword in [".in_(", "in_op", "__eq__", "__ne__", "__lt__", "__gt__"]
                ):
                    warn_migration_issue(
                        f"Query object being evaluated in comparison context. "
                        f"In SQLAlchemy 1.4, use .scalar_subquery() for subqueries in in_() operations.",
                        "QUERY_IN_COMPARISON",
                    )
                    break

            return original_query_iter(self)

        Query.__iter__ = patched_query_iter

    except ImportError:
        pass


def patch_column_in_operations():
    """Patch Column.in_() to detect problematic arguments"""
    try:
        from sqlalchemy.orm.query import Query
        from sqlalchemy.sql.elements import ColumnElement

        # Store original in_() method
        original_in = ColumnElement.in_

        @wraps(original_in)
        def patched_in(self, other):
            # Check if 'other' is a Query object
            if isinstance(other, Query):
                warn_migration_issue(
                    f"Query object passed directly to in_() operation. "
                    f"In SQLAlchemy 1.4, you'll need to use .scalar_subquery(): "
                    f"column.in_(query.scalar_subquery())",
                    "QUERY_IN_IN_CLAUSE",
                )

            # Check if 'other' is iterable and contains Row-like objects
            try:
                if hasattr(other, "__iter__") and not isinstance(other, (str, bytes)):
                    first_item = next(iter(other), None)
                    if first_item is not None:
                        # Check if it looks like a Row object
                        if hasattr(first_item, "_fields") or hasattr(first_item, "keys"):
                            warn_migration_issue(
                                f"Row-like objects in in_() clause. "
                                f"In SQLAlchemy 1.4, extract scalar values first.",
                                "ROW_IN_IN_CLAUSE",
                            )
            except (TypeError, StopIteration):
                pass

            return original_in(self, other)

        ColumnElement.in_ = patched_in

    except ImportError:
        pass


def patch_subquery_usage():
    """Patch subquery() method to detect usage patterns"""
    try:
        from sqlalchemy.orm.query import Query

        original_subquery = Query.subquery

        @wraps(original_subquery)
        def patched_subquery(self, name=None):
            warn_migration_issue(
                f"Query.subquery() usage detected. "
                f"In SQLAlchemy 1.4, prefer .scalar_subquery() for scalar subqueries "
                f"or wrap with select() for in_() operations: select(subquery.c.column)",
                "SUBQUERY_USAGE",
            )
            return original_subquery(self, name)

        Query.subquery = patched_subquery

    except ImportError:
        pass


def apply_all_patches():
    """Apply all monkey patches"""
    try:
        patch_row_comparisons()
        patch_query_in_operations()
        patch_column_in_operations()
        patch_subquery_usage()

        logger.info("SQLAlchemy 1.3->1.4 migration detector patches applied successfully")

    except Exception as e:
        logger.error(f"Failed to apply migration detector patches: {e}")


def configure_logging(level=logging.WARNING, filename=None):
    """Configure logging for migration warnings"""
    logger.setLevel(level)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create new handler
    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s [MIGRATION WARNING] %(message)s\n" "File: %(pathname)s:%(lineno)d\n" + "-" * 80
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def enable_migration_detector(log_level=logging.WARNING, log_file=None):
    """
    Enable the migration detector with optional logging configuration

    Args:
        log_level: Logging level (default: WARNING)
        log_file: Optional file to write logs to (default: stderr)
    """
    configure_logging(log_level, log_file)
    apply_all_patches()


# Auto-apply patches when module is imported
if __name__ != "__main__":
    apply_all_patches()


# Example usage function
def example_usage():
    """
    Example of how to use this migration detector
    """
    print("SQLAlchemy 1.3 -> 1.4 Migration Detector")
    print("=" * 50)
    print()
    print("1. Import this module BEFORE SQLAlchemy:")
    print("   import sqlalchemy_migration_detector")
    print("   import sqlalchemy")
    print()
    print("2. Optional: Configure logging:")
    print("   sqlalchemy_migration_detector.configure_logging(")
    print("       level=logging.INFO,")
    print("       filename='migration_warnings.log'")
    print("   )")
    print()
    print("3. Run your application normally")
    print("4. Check logs for migration warnings")
    print()
    print("Warning Categories:")
    print("- ROW_COMPARISON: Row objects in comparisons")
    print("- QUERY_IN_COMPARISON: Query objects in comparisons")
    print("- QUERY_IN_IN_CLAUSE: Query objects in in_() clauses")
    print("- ROW_IN_IN_CLAUSE: Row objects in in_() clauses")
    print("- SUBQUERY_USAGE: .subquery() method usage")


if __name__ == "__main__":
    example_usage()
