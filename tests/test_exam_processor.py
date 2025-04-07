import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import json
from app.exam_processor import process_exams

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("ExamProcessorTest") \
        .master("local[2]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_exam_data(tmp_path):
    """Create a temporary JSON file with sample exam data."""
    data = [
        {
            "student_name": "Alice",
            "score": 85.5,
            "subject": "Mathematics",
            "date": "2023-01-15"
        },
        {
            "student_name": "Bob",
            "score": 72.0,
            "subject": "Mathematics",
            "date": "2023-01-15"
        }
    ]
    
    # Create a temporary directory for the test data
    test_data_dir = tmp_path / "test_data"
    test_data_dir.mkdir()
    
    # Write the sample data to a JSON file
    input_file = test_data_dir / "exams.json"
    with open(input_file, "w") as f:
        json.dump(data, f)
    
    return str(input_file)

def test_process_exams(spark, sample_exam_data, tmp_path):
    """Test the process_exams function with sample data."""
    # Define the output path
    output_path = str(tmp_path / "exam_report")
    
    # Process the exams
    report = process_exams(spark, sample_exam_data, output_path)
    
    # Verify the report contents using dot notation for Pydantic model
    assert report.total_exams == 2
    assert report.passed_students == 2  # Both scores are above 60
    assert report.failed_students == 0
    assert report.best_student == "Alice"
    assert report.worst_student == "Bob"
    assert report.average_score == 78.75  # (85.5 + 72.0) / 2
    
    # Verify that the Delta table was created
    assert os.path.exists(output_path)
    
    # Read the Delta table and verify its contents
    df = spark.read.format("delta").load(output_path)
    assert df.count() == 1  # Should have one row with the report
    
    # Define the expected schema
    expected_schema = StructType([
        StructField("total_exams", LongType(), True),
        StructField("passed_students", LongType(), True),
        StructField("failed_students", LongType(), True),
        StructField("best_student", StringType(), True),
        StructField("worst_student", StringType(), True),
        StructField("average_score", DoubleType(), True)
    ])
    
    # Compare schemas field by field
    actual_schema = df.schema
    assert len(actual_schema) == len(expected_schema), "Schema field count mismatch"
    
    # Create dictionaries for easier comparison
    actual_fields = {field.name: field for field in actual_schema}
    expected_fields = {field.name: field for field in expected_schema}
    
    # Compare each field
    for field_name, expected_field in expected_fields.items():
        assert field_name in actual_fields, f"Missing field: {field_name}"
        actual_field = actual_fields[field_name]
        assert actual_field.dataType == expected_field.dataType, f"Type mismatch for field {field_name}: expected {expected_field.dataType}, got {actual_field.dataType}"
        assert actual_field.nullable == expected_field.nullable, f"Nullability mismatch for field {field_name}" 