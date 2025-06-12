#!/usr/bin/env python3
"""
SQLAlchemy 1.3 -> 1.4 Migration Helper
Detects Query objects used in in_() that may need .scalar_subquery() added
"""

import argparse
import ast
import os
import sys
from pathlib import Path


class QueryInFinder(ast.NodeVisitor):
    def __init__(self, filepath):
        self.filepath = filepath
        self.found_patterns = []

    def visit_Call(self, node):
        # Look for .in_() calls
        if (
            hasattr(node.func, "attr")
            and node.func.attr in ["in_", "notin_"]
            and len(node.args) > 0
        ):
            arg = node.args[0]
            pattern_type = self.classify_argument(arg)

            if pattern_type:
                code = self.safe_unparse(node)

                self.found_patterns.append(
                    {
                        "line": node.lineno,
                        "code": code,
                        "type": pattern_type,
                        "arg": self.safe_unparse(arg),
                        "comparison": "in_or_notin_",
                    }
                )

        self.generic_visit(node)

    def visit_Compare(self, node):
        # Look for equality/inequality comparisons
        for i, op in enumerate(node.ops):
            if isinstance(op, (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)):
                # Check the right side of the comparison
                if i < len(node.comparators):
                    comparator = node.comparators[i]
                    pattern_type = self.classify_comparison_argument(comparator)

                    if pattern_type:
                        code = self.safe_unparse(node)

                        op_symbol = {
                            ast.Eq: "==",
                            ast.NotEq: "!=",
                            ast.Lt: "<",
                            ast.LtE: "<=",
                            ast.Gt: ">",
                            ast.GtE: ">=",
                        }.get(type(op), "?")

                        self.found_patterns.append(
                            {
                                "line": node.lineno,
                                "code": code,
                                "type": pattern_type,
                                "arg": self.safe_unparse(comparator),
                                "comparison": op_symbol,
                            }
                        )

        self.generic_visit(node)

    def classify_argument(self, arg):
        """Classify the type of argument passed to in_()"""

        # Direct method calls like session.query(...).filter(...)
        if isinstance(arg, ast.Call):
            if self.is_query_chain(arg):
                return "direct_query"
            elif self.has_subquery_call(arg):
                return "subquery_call"

        # Variable names that might be queries
        elif isinstance(arg, ast.Name):
            name = arg.id.lower()
            if any(
                (
                    *[word is name for word in ["q"]],
                    *[word in name for word in ["query", "subq", "sub_q"]],
                )
            ):
                return "query_variable"

        # Attribute access like obj.some_query
        elif isinstance(arg, ast.Attribute):
            attr = arg.attr.lower()
            if any(
                (
                    *[word is attr for word in ["q"]],
                    *[word in attr for word in ["query", "subq", "sub_q"]],
                )
            ):
                return "query_attribute"

        return None

    def classify_comparison_argument(self, arg):
        """Classify arguments in comparison operations that might be Row objects"""

        # Method calls that return Row objects
        if isinstance(arg, ast.Call):
            if self.is_query_chain_with_row_result(arg):
                return "row_result"

        # Variables that might contain Row objects
        elif isinstance(arg, ast.Name):
            name = arg.id.lower()
            if any(
                word in name
                for word in ["row", "first", "one", "result", "record", "data"]
            ):
                return "possible_row_variable"

        # Attribute access that might be Row objects
        elif isinstance(arg, ast.Attribute):
            attr = arg.attr.lower()
            if any(
                word in attr
                for word in ["row", "first", "one", "result", "record", "data"]
            ):
                return "possible_row_attribute"

        return None

    def is_query_chain(self, node):
        """Check if this looks like a session.query(...) chain"""
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                # Check for .filter, .join, etc. method calls
                if node.func.attr in [
                    "filter",
                    "join",
                    "outerjoin",
                    "group_by",
                    "having",
                    "order_by",
                ]:
                    return self.is_query_chain(node.func.value)
                # Check for .query method
                elif node.func.attr == "query":
                    return True
        return False

    def is_query_chain_with_row_result(self, node):
        """Check if this is a query chain that returns a Row (first, one, etc.)"""
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                # Check for methods that return Row objects
                if node.func.attr in [
                    "first",
                    "one",
                    "one_or_none",
                    "scalar_one",
                    "scalar_one_or_none",
                ]:
                    return self.is_query_chain(node.func.value)
                # Check for other query methods
                elif node.func.attr in [
                    "filter",
                    "join",
                    "outerjoin",
                    "group_by",
                    "having",
                    "order_by",
                    "limit",
                ]:
                    return self.is_query_chain_with_row_result(node.func.value)
                # Check for .query method
                elif node.func.attr == "query":
                    return True
        return False

    def has_subquery_call(self, node):
        """Check if the call chain includes .subquery() or .scalar_subquery()"""
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in ["subquery", "scalar_subquery", "exists", "all"]:
                    return True
                return self.has_subquery_call(node.func.value)
        return False

    def safe_unparse(self, node):
        """Safe unparsing that works with different Python versions"""
        try:
            return ast.unparse(node)
        except AttributeError:
            # Fallback for Python < 3.9
            return self.fallback_unparse(node)

    def fallback_unparse(self, node):
        """Fallback unparsing for Python < 3.9"""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            value_str = self.fallback_unparse(node.value)
            return f"{value_str}.{node.attr}"
        elif isinstance(node, ast.Call):
            func_str = self.fallback_unparse(node.func)
            args_str = ", ".join(
                self.fallback_unparse(arg) for arg in node.args[:2]
            )  # Limit args
            if len(node.args) > 2:
                args_str += ", ..."
            return f"{func_str}({args_str})"
        elif isinstance(node, ast.Compare):
            left = self.fallback_unparse(node.left)
            comparisons = []
            for op, comp in zip(node.ops, node.comparators):
                op_str = {
                    ast.Eq: "==",
                    ast.NotEq: "!=",
                    ast.Lt: "<",
                    ast.LtE: "<=",
                    ast.Gt: ">",
                    ast.GtE: ">=",
                }.get(type(op), "?")
                comp_str = self.fallback_unparse(comp)
                comparisons.append(f"{op_str} {comp_str}")
            return f"{left} {' '.join(comparisons)}"
        else:
            return f"<{type(node).__name__}>"


def scan_file(filepath):
    """Scan a single Python file for problematic patterns"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content)
        finder = QueryInFinder(filepath)
        finder.visit(tree)

        return finder.found_patterns
    except Exception as e:
        print(f"Error scanning {filepath}: {e}", file=sys.stderr)
        return []


def scan_directory(directory, extensions=None):
    """Recursively scan directory for Python files"""
    if extensions is None:
        extensions = {".py"}

    results = {}
    directory = Path(directory)

    if not directory.exists():
        print(f"Error: Directory '{directory}' does not exist", file=sys.stderr)
        return results

    if not directory.is_dir():
        print(f"Error: '{directory}' is not a directory", file=sys.stderr)
        return results

    for filepath in directory.rglob("*"):
        if filepath.is_file() and filepath.suffix in extensions:
            patterns = scan_file(filepath)
            if patterns:
                results[str(filepath)] = patterns

    return results


def print_results(results, show_details=True):
    """Print scan results in a readable format"""
    if not results:
        print("✅ No problematic patterns found!")
        return

    total_files = len(results)
    total_patterns = sum(len(patterns) for patterns in results.values())

    print(f"🔍 Found {total_patterns} potential issues in {total_files} files:\n")

    for filepath, patterns in results.items():
        print(f"📄 {filepath}:")

        for pattern in patterns:
            icon = {
                "direct_query": "🚨",
                "query_variable": "⚠️ ",
                "query_attribute": "⚠️ ",
                "subquery_call": "ℹ️ ",
                "row_result": "🔴",
                "possible_row_variable": "🟡",
                "possible_row_attribute": "🟡",
            }.get(pattern["type"], "❓")

            comparison = pattern.get("comparison", "?")
            print(f"  {icon} Line {pattern['line']}: {pattern['type']} ({comparison})")
            if show_details:
                print(f"     Code: {pattern['code']}")
                print(f"     Arg:  {pattern['arg']}")
            print()

    print("📋 Summary:")
    print("🚨 direct_query: Query in in_() - needs .scalar_subquery()")
    print("🔴 row_result: Row object in comparison - needs .scalar() or [0]")
    print("🟡 possible_row_*: Variables that might be Row objects")
    print("⚠️  *_variable/*_attribute: Likely needs investigation")
    print("ℹ️  subquery_call: Already has .subquery() - check for warnings")


def main():
    parser = argparse.ArgumentParser(
        description="Detect SQLAlchemy Query objects in in_() that may need migration"
    )
    parser.add_argument("directory", help="Directory to scan recursively")
    parser.add_argument(
        "--extensions",
        nargs="+",
        default=[".py"],
        help="File extensions to scan (default: .py)",
    )
    parser.add_argument(
        "--no-details", action="store_true", help="Hide detailed code snippets"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show only file counts, no individual patterns",
    )

    args = parser.parse_args()

    print(f"🔍 Scanning {args.directory} for SQLAlchemy migration patterns...")
    print(f"📁 Extensions: {', '.join(args.extensions)}\n")

    results = scan_directory(args.directory, set(args.extensions))

    if args.summary_only:
        if results:
            total_patterns = sum(len(patterns) for patterns in results.values())
            print(f"Found {total_patterns} patterns in {len(results)} files")
            for filepath in results:
                print(f"  {filepath}: {len(results[filepath])} patterns")
        else:
            print("No patterns found")
    else:
        print_results(results, show_details=not args.no_details)


if __name__ == "__main__":
    main()
