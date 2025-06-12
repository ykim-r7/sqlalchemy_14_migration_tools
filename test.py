#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "sqlalchemy==1.3.24",
#     "mysqlclient==2.0.3",
#     "cryptography>=3.0.0",
# ]
# ///
"""
SQLAlchemy Row Object Binding Demo

This script demonstrates issues with Row objects in comparison operations
that can occur in various SQLAlchemy versions, particularly around in_() and == operations.

The core issue: Row objects (tuples returned by queries) sometimes can't be
automatically coerced to scalar values for comparisons, leading to either:
1. Silent failures (no results returned)
2. Binding errors: "Error binding parameter X - probably unsupported type"

Usage:
  # Install uv first: curl -LsSf https://astral.sh/uv/install.sh | sh
  # Then run this script directly:
  uv run sqlalchemy_demo.py

  # To test with SQLAlchemy 1.3, edit the dependencies above:
  # "sqlalchemy==1.3.24",

Database Setup:
  You'll need a MySQL server running with a test database.
  Update the connection string below with your MySQL credentials.

  Example MySQL setup:
    CREATE DATABASE test_db;
    # Update connection string: mysql+pymysql://username:password@host/database

  The script will automatically fall back to SQLite if MySQL is not available.
"""

import sqlalchemy as sa
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

print(f"SQLAlchemy Version: {sa.__version__}")
print("=" * 50)

# Setup
Base = declarative_base()


class Department(Base):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    employees = relationship("Employee", back_populates="department")


class Employee(Base):
    __tablename__ = "employees"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    department_id = Column(Integer, ForeignKey("departments.id"))
    department = relationship("Department", back_populates="employees")


# Create MySQL database connection
# Update this connection string with your MySQL credentials
# Format: mysql+pymysql://username:password@host:port/database
# For older MySQL auth, you can add: ?auth_plugin=mysql_native_password
MYSQL_URL = "mysql+mysqldb://root:@localhost:3306/test_db"

print(f"Attempting to connect to MySQL: {MYSQL_URL.replace('password', '***')}")
try:
    engine = create_engine(MYSQL_URL, echo=False)
    # Test the connection
    with engine.connect() as conn:
        conn.execute(sa.text("SELECT 1"))
    print("✅ Connected to MySQL database")
    db_type = "MySQL"
except Exception as e:
    print(f"❌ MySQL connection failed: {e}")
    if "cryptography" in str(e):
        print("💡 Note: cryptography package is now included in dependencies")
    if "auth" in str(e).lower():
        print("💡 Try adding ?auth_plugin=mysql_native_password to your MySQL URL")
    print("📝 Using SQLite fallback")
    print("💡 To use MySQL: Update MYSQL_URL variable with your credentials")
    engine = create_engine("sqlite:///:memory:", echo=False)
    db_type = "SQLite"
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Insert test data
# dept1 = Department(id=1, name="Engineering")
# dept2 = Department(id=2, name="Marketing")
# dept3 = Department(id=3, name="Sales")

# emp1 = Employee(id=1, name="Alice", department_id=1)
# emp2 = Employee(id=2, name="Bob", department_id=1)
# emp3 = Employee(id=3, name="Charlie", department_id=2)
# emp4 = Employee(id=4, name="Diana", department_id=3)

# session.add_all([dept1, dept2, dept3, emp1, emp2, emp3, emp4])
# session.commit()

print("Test Data Created:")
print("Departments: Engineering(1), Marketing(2), Sales(3)")
print("Employees: Alice(1,dept1), Bob(2,dept1), Charlie(3,dept2), Diana(4,dept3)")
print(f"Database: {db_type}")
print()


def run_test(test_name, query_func, expected_count=None):
    """Helper to run a test and show results"""
    print(f"🔍 {test_name}")
    try:
        results = query_func()
        if hasattr(results, "count"):
            count = results.count()
        elif hasattr(results, "__len__"):
            count = len(list(results))
        else:
            count = len(list(results)) if results else 0

        print(f"   ✅ Found {count} results")
        if expected_count is not None and count != expected_count:
            print(f"   ⚠️  Expected {expected_count}, got {count} - POTENTIAL ISSUE!")

        # Show first few results
        if hasattr(results, "limit"):
            sample = results.limit(3).all()
        else:
            sample = list(results)[:3] if results else []

        for item in sample:
            if hasattr(item, "name"):
                print(f"      - {item.name}")
            else:
                print(f"      - {item}")

    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        if "Error binding parameter" in str(e) and "probably unsupported type" in str(
            e
        ):
            print("   🎯 This is the SQLAlchemy 1.4 Row binding issue!")
            print("   💡 SQLAlchemy is trying to bind a Row object instead of a scalar")
    print()


# Test 1: in_() with subquery (PROBLEMATIC)
print("=" * 50)
print("TEST 1: in_() with subquery - Row objects (CAN FAIL)")
print("=" * 50)


def test1_problematic():
    """This may work or fail depending on version and circumstances"""
    subquery = session.query(Department.id).filter(
        Department.name.in_(["Engineering", "Marketing"])
    )
    return session.query(Employee).filter(Employee.department_id.in_(subquery))


# run_test("Employees in Engineering/Marketing depts (PROBLEMATIC)", test1_problematic, 3)

# Test 2: in_() with .all() (PROBLEMATIC)
print("TEST 2: in_() with .all() - Row objects (CAN FAIL)")
print("=" * 50)


def test2_problematic():
    """This may work or fail depending on version and circumstances"""
    subquery = session.query(Department.id).filter(
        Department.name.in_(["Engineering", "Marketing"])
    )

    return session.query(Employee).filter(Employee.department_id.in_(subquery.all()))


# run_test(
#     "Employees in Engineering/Marketing depts via .all() (PROBLEMATIC)",
#     test2_problematic,
#     3,
# )

# Test 3: == with Row object (PROBLEMATIC)
print("TEST 3: == comparison with Row object (CAN FAIL)")
print("=" * 50)


def test3_problematic():
    """This may work or fail depending on version and circumstances"""
    first_dept_row = (
        session.query(Department.id).filter(Department.name == "Engineering").first()
    )
    return session.query(Employee).filter(Employee.department_id == first_dept_row)


run_test(
    "Employees in first dept via Row comparison (PROBLEMATIC)", test3_problematic, 2
)

# FIXES for SQLAlchemy 1.4
print("=" * 50)
print("FIXES - These work in both 1.3 and 1.4")
print("=" * 50)


# Fix 1: Use scalar_subquery()
def fix1_scalar_subquery():
    """Works in 1.4, may work in 1.3 depending on version"""
    if hasattr(session.query(Department.id), "scalar_subquery"):
        subquery = (
            session.query(Department.id)
            .filter(Department.name.in_(["Engineering", "Marketing"]))
            .scalar_subquery()
        )
        return session.query(Employee).filter(Employee.department_id.in_(subquery))
    else:
        print("   ⚠️  scalar_subquery() not available in this version")
        return session.query(Employee).filter(Employee.id == -1)  # Return empty


run_test("FIX 1: Using scalar_subquery()", fix1_scalar_subquery, 3)


# Fix 2: Use scalars()
def fix2_scalars():
    """Works in 1.4, may not work in 1.3"""
    try:
        if hasattr(session.query(Department.id), "scalars"):
            dept_ids = (
                session.query(Department.id)
                .filter(Department.name.in_(["Engineering", "Marketing"]))
                .scalars()
            )
            return session.query(Employee).filter(Employee.department_id.in_(dept_ids))
        else:
            print("   ⚠️  scalars() not available in this version")
            return session.query(Employee).filter(Employee.id == -1)  # Return empty
    except Exception as e:
        print(f"   ⚠️  scalars() failed: {e}")
        return session.query(Employee).filter(Employee.id == -1)  # Return empty


run_test("FIX 2: Using scalars()", fix2_scalars, 3)


# Fix 3: Manual scalar extraction (Works in both)
def fix3_manual_extraction():
    """Works in both 1.3 and 1.4"""
    dept_rows = (
        session.query(Department.id)
        .filter(Department.name.in_(["Engineering", "Marketing"]))
        .all()
    )
    dept_ids = [row[0] for row in dept_rows]  # Extract scalar values
    return session.query(Employee).filter(Employee.department_id.in_(dept_ids))


run_test("FIX 3: Manual scalar extraction [row[0]]", fix3_manual_extraction, 3)


# Fix 4: Query objects directly (Works in both)
def fix4_query_objects():
    """Works in both 1.3 and 1.4"""
    depts = (
        session.query(Department)
        .filter(Department.name.in_(["Engineering", "Marketing"]))
        .all()
    )
    dept_ids = [dept.id for dept in depts]  # Extract IDs from objects
    return session.query(Employee).filter(Employee.department_id.in_(dept_ids))


run_test("FIX 4: Query objects and extract IDs", fix4_query_objects, 3)


# Fix 5: Generator expression (Works in both)
def fix5_generator():
    """Works in both 1.3 and 1.4"""
    dept_ids = (
        row[0]
        for row in session.query(Department.id).filter(
            Department.name.in_(["Engineering", "Marketing"])
        )
    )
    return session.query(Employee).filter(Employee.department_id.in_(dept_ids))


run_test("FIX 5: Generator expression", fix5_generator, 3)


# Fix 6: Fix == comparison
def fix6_equality():
    """Works in both 1.3 and 1.4"""
    first_dept_row = (
        session.query(Department.id).filter(Department.name == "Engineering").first()
    )
    dept_id = first_dept_row[0] if first_dept_row else None  # Extract scalar
    if dept_id:
        return session.query(Employee).filter(Employee.department_id == dept_id)
    else:
        return session.query(Employee).filter(Employee.id == -1)  # Return empty


run_test("FIX 6: == with scalar extraction", fix6_equality, 2)

print("=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Database tested: {db_type}")
print("Row object binding issues can occur in multiple SQLAlchemy versions.")
print("The behavior depends on:")
print("- SQLAlchemy version (stricter in 1.4+)")
print("- Database backend (MySQL generally more forgiving than SQLite)")
print("- Query complexity and result types")
print()
print("Symptoms you might see:")
print("- 'Error binding parameter X - probably unsupported type'")
print("- Parameters showing tuples like ((1,),) instead of scalars like (1,)")
print("- Silent empty results with no errors")
print("- Different behavior between SQLite and MySQL")
print()
print("Best practices:")
print("1. Always extract scalars explicitly: row[0] or row.column_name")
print("2. Use .scalars() method when available")
print("3. Use .scalar_subquery() for subqueries in comparisons")
print("4. Test thoroughly with your exact SQLAlchemy version and database")
print("5. Avoid direct Row object comparisons - always extract the value you need")
print("6. MySQL tends to be more forgiving than SQLite for these issues")
print()
print("To test different SQLAlchemy versions:")
print("- Edit the dependencies in the script header")
print("- Change 'sqlalchemy==1.4.53' to 'sqlalchemy==1.3.24'")
print("- Run: uv run sqlalchemy_demo.py")

session.close()
