import re
from pyspark.sql.types import (
    StringType, LongType, IntegerType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, DecimalType
)

# Pattern to match any decimal(precision, scale) from metadata
_DECIMAL_PATTERN = re.compile(r'^decimal\((\d+),\s*(\d+)\)$', re.IGNORECASE)

def map_data_types(data_types):
    """
    Maps metadata string data types to PySpark types.
    Handles decimal(p,s) generically — e.g. decimal(18,2), decimal(38,18).
    """
    spark_data_types = []
    for data_type in data_types:
        dt = data_type.strip().lower()

        if dt == 'string':
            spark_data_type = StringType()
        elif dt == 'long':
            spark_data_type = LongType()
        elif dt in ('integer', 'int'):
            spark_data_type = IntegerType()
        elif dt == 'double':
            spark_data_type = DoubleType()
        elif dt == 'float':
            spark_data_type = FloatType()
        elif dt == 'boolean':
            spark_data_type = BooleanType()
        elif dt == 'timestamp':
            spark_data_type = TimestampType()
        elif dt == 'date':
            spark_data_type = DateType()
        elif _DECIMAL_PATTERN.match(dt):
            # Dynamically extract precision and scale — works for any decimal(p,s)
            m = _DECIMAL_PATTERN.match(dt)
            precision, scale = int(m.group(1)), int(m.group(2))
            spark_data_type = DecimalType(precision, scale)
        else:
            raise ValueError(f"Invalid data type: {data_type}")

        spark_data_types.append(spark_data_type)

    return spark_data_types
