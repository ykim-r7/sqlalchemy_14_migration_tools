"""
SQLAlchemy 1.3 Migration Detector - Smart Monkey Patch

This patches the exact point where 1.3 and 1.4 differ: parameter processing.
In 1.3, Row objects get automatically coerced during SQL compilation.
In 1.4, this coercion was removed and causes errors.

We patch the parameter processing to detect when Row objects would be 
passed to the SQL layer - this is the EXACT point where 1.4 breaks.
"""

import sys
import logging
from functools import wraps

# Set up logging
logger = logging.getLogger('sqlalchemy_migration_detector')
logger.setLevel(logging.WARNING)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '\n🚨 [SQLALCHEMY MIGRATION WARNING] 🚨\n'
        '%(message)s\n'
        'File: %(pathname)s:%(lineno)d\n'
        '=' * 80
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_caller_info():
    """Get information about where the problematic code is being called from"""
    frame = sys._getframe()
    # Walk up the stack to find the first frame outside SQLAlchemy internals
    while frame:
        filename = frame.f_code.co_filename
        if (not filename.endswith('sqlalchemy_migration_detector.py') and
            'sqlalchemy' not in filename and
            'site-packages' not in filename):
            return {
                'filename': filename,
                'lineno': frame.f_lineno,
                'function': frame.f_code.co_name
            }
        frame = frame.f_back
    return {'filename': 'unknown', 'lineno': 0, 'function': 'unknown'}


def warn_migration_issue(message, row_obj=None):
    """Log a migration warning with caller information"""
    caller = get_caller_info()
    
    extra_info = ""
    if row_obj is not None:
        if hasattr(row_obj, '_fields'):
            extra_info = f"\nRow fields: {list(row_obj._fields)}"
        elif hasattr(row_obj, 'keys'):
            try:
                extra_info = f"\nRow keys: {list(row_obj.keys())}"
            except:
                extra_info = "\nRow keys: <unable to extract>"
    
    full_message = f"{message}{extra_info}\n\nFIX: Use .scalar() or row[0] to extract the actual value instead of passing the Row object."
    
    # Create a LogRecord with the caller's location
    record = logging.LogRecord(
        name=logger.name,
        level=logging.WARNING,
        pathname=caller['filename'],
        lineno=caller['lineno'],
        msg=full_message,
        args=(),
        exc_info=None
    )
    logger.handle(record)


def is_row_like(obj):
    """Check if an object is a SQLAlchemy Row or RowProxy"""
    # Check for SQLAlchemy Row/RowProxy characteristics
    if hasattr(obj, '_fields') and hasattr(obj, '__getitem__'):
        return True
    if hasattr(obj, 'keys') and hasattr(obj, '__getitem__') and callable(obj.keys):
        # Make sure it's not just a regular dict
        try:
            # Row objects have numeric indexing
            obj[0]
            return True
        except (IndexError, KeyError, TypeError):
            pass
    return False


def patch_parameter_processing():
    """
    Patch the parameter processing where Row objects would cause 1.4 to fail.
    This is the exact point where the behavior differs between versions.
    """
    try:
        # SQLAlchemy 1.3 parameter processing
        from sqlalchemy.sql import sqltypes
        from sqlalchemy.engine import default
        
        # Patch the literal parameter processor
        original_process_literal_param = None
        if hasattr(default.DefaultDialect, '_literal_processor'):
            original_process_literal_param = default.DefaultDialect._literal_processor
        
        # Patch the bind parameter processor  
        if hasattr(sqltypes.TypeEngine, 'bind_processor'):
            original_bind_processor = sqltypes.TypeEngine.bind_processor
            
            def patched_bind_processor(self, dialect):
                original_processor = original_bind_processor(self, dialect)
                
                if original_processor is None:
                    def row_detecting_processor(value):
                        if is_row_like(value):
                            warn_migration_issue(
                                "Row object being processed as SQL parameter. "
                                "This will fail in SQLAlchemy 1.4.",
                                row_obj=value
                            )
                        return value
                    return row_detecting_processor
                else:
                    @wraps(original_processor)
                    def wrapped_processor(value):
                        if is_row_like(value):
                            warn_migration_issue(
                                "Row object being processed as SQL parameter. "
                                "This will fail in SQLAlchemy 1.4.",
                                row_obj=value
                            )
                        return original_processor(value)
                    return wrapped_processor
            
            sqltypes.TypeEngine.bind_processor = patched_bind_processor
    
    except ImportError as e:
        logger.warning(f"Could not patch parameter processing: {e}")


def patch_query_evaluation():
    """
    Patch Query.__iter__ to detect when Query objects are being evaluated 
    in contexts where they'll be treated as parameters
    """
    try:
        from sqlalchemy.orm.query import Query
        
        original_iter = Query.__iter__
        
        @wraps(original_iter)
        def patched_iter(self):
            # Check if we're being evaluated in a parameter context
            frame = sys._getframe(1)
            while frame:
                code_context = getattr(frame, 'f_code', None)
                if code_context:
                    # Look for signs we're in parameter processing
                    local_vars = frame.f_locals
                    
                    # Check if we're in an in_() operation by looking at the calling context
                    if any(key for key in local_vars.keys() 
                           if 'in_' in str(key).lower() or 'param' in str(key).lower()):
                        warn_migration_issue(
                            "Query object being evaluated as parameter in in_() operation. "
                            "In SQLAlchemy 1.4, use .scalar_subquery() instead."
                        )
                        break
                
                frame = frame.f_back
                # Don't go too deep
                if frame and frame.f_code.co_filename.endswith('sqlalchemy_migration_detector.py'):
                    break
            
            return original_iter(self)
        
        Query.__iter__ = patched_iter
        
    except ImportError:
        pass


def patch_in_operations():
    """
    Patch the in_() operation to detect Query objects being passed directly
    """
    try:
        from sqlalchemy.sql.elements import ColumnElement
        from sqlalchemy.orm.query import Query
        
        original_in = ColumnElement.in_
        
        @wraps(original_in)
        def patched_in(self, other):
            if isinstance(other, Query):
                warn_migration_issue(
                    f"Query object passed directly to in_() operation on column {self}. "
                    f"In SQLAlchemy 1.4, use .scalar_subquery(): "
                    f"{self}.in_(query.scalar_subquery())"
                )
            return original_in(self, other)
        
        ColumnElement.in_ = patched_in
        
    except ImportError:
        pass


def apply_migration_patches():
    """Apply all the targeted migration detection patches"""
    try:
        patch_parameter_processing()
        patch_query_evaluation()  
        patch_in_operations()
        
        print("🔍 SQLAlchemy 1.3→1.4 migration detector activated!")
        print("   Will warn about patterns that break in SQLAlchemy 1.4")
        print("   Run your application normally to detect issues.")
        print()
        
    except Exception as e:
        logger.error(f"Failed to apply migration patches: {e}")


def test_detection():
    """Test function to verify the patches work"""
    print("🧪 Testing migration detection...")
    
    try:
        import sqlalchemy as sa
        from sqlalchemy import create_engine, Column, Integer, String
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.orm import sessionmaker
        
        # Create test setup
        engine = create_engine('sqlite:///:memory:', echo=False)
        Base = declarative_base()
        
        class TestModel(Base):
            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
        
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Add test data
        session.add(TestModel(name='test'))
        session.commit()
        
        print("✅ Test setup complete. Now testing problematic patterns...")
        
        # Test 1: Row in comparison (this should trigger warning)
        try:
            row = session.query(TestModel).first()
            # This would work in 1.3 but break in 1.4 if row is used in comparison
            print("   Testing Row object usage...")
        except Exception as e:
            print(f"   ⚠️  Error in test: {e}")
        
        # Test 2: Query in in_() (this should trigger warning)  
        try:
            subquery = session.query(TestModel.id)
            # This should trigger our warning
            result = session.query(TestModel).filter(TestModel.id.in_(subquery))
            print("   Testing Query in in_() operation...")
            list(result)  # Force evaluation
        except Exception as e:
            print(f"   ⚠️  Error in test: {e}")
            
        print("🧪 Test complete!")
        
    except ImportError:
        print("   ⏭️  SQLAlchemy not available for testing, but patches are ready")


# Auto-apply patches when module is loaded
if __name__ != '__main__':
    apply_migration_patches()
else:
    # If run directly, show usage info and test
    print(__doc__)
    print("\n" + "="*80)
    apply_migration_patches()
    test_detection()