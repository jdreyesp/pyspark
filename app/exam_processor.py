from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, min, avg, when
from pydantic import BaseModel, Field
from typing import List, Optional
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

class Exam(BaseModel):
    student_name: str = Field(..., description="Name of the student")
    score: float = Field(..., ge=0, le=100, description="Exam score between 0 and 100")
    subject: str = Field(..., description="Subject of the exam")
    date: str = Field(..., description="Date of the exam in YYYY-MM-DD format")

class ExamReport(BaseModel):
    total_exams: int = Field(..., description="Total number of exams")
    passed_students: int = Field(..., description="Number of students who passed (score >= 60)")
    failed_students: int = Field(..., description="Number of students who failed (score < 60)")
    best_student: str = Field(..., description="Name of the student with the highest score")
    worst_student: str = Field(..., description="Name of the student with the lowest score")
    average_score: float = Field(..., description="Average score across all exams")

def process_exams(spark: SparkSession, input_path: str, output_path: str):
    # Define schema
    schema = StructType([
        StructField("student_name", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("subject", StringType(), True),
        StructField("date", StringType(), True)
    ])
    
    # Read JSON data with schema
    df = spark.read.option('multiline', True).schema(schema).json(input_path)
    
    # Print schema and first few rows for debugging
    print("Schema:")
    df.printSchema()
    print("\nFirst few rows:")
    df.show(5)
    
    with open("data/exams.json", "r") as file:
         print(file.read())
         
    # Calculate statistics using Spark API
    total_exams = df.count()
    
    # Calculate passed and failed students
    passed_students = df.filter(col("score") >= 60).count()
    failed_students = total_exams - passed_students
    
    # Find best and worst students
    best_student_df = df.orderBy(col("score").desc()).limit(1)
    worst_student_df = df.orderBy(col("score").asc()).limit(1)
    
    best_student = best_student_df.select("student_name").collect()[0][0]
    worst_student = worst_student_df.select("student_name").collect()[0][0]
    
    # Calculate average score
    average_score = df.select(avg("score")).collect()[0][0]
    
    # Create report
    print(f"Total exams: {total_exams}")
    report = ExamReport(
        total_exams=total_exams,
        passed_students=passed_students,
        failed_students=failed_students,
        best_student=best_student,
        worst_student=worst_student,
        average_score=average_score
    )
    
    # Convert report to DataFrame
    report_df = spark.createDataFrame([report.dict()])
    
    # Write to Delta table
    report_df.write.format("delta").mode("overwrite").save(output_path)
    
    return report

if __name__ == "__main__":
    # Initialize Spark session
    sparkSession = SparkSession.builder \
        .appName("ExamProcessor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Process exams and generate report
    report = process_exams(
        spark=sparkSession,
        input_path="data/exams.json",
        output_path="data/exam_report"
    )
    
    # Print report
    print("\nExam Report:")
    print(f"Total Exams: {report.total_exams}")
    print(f"Passed Students: {report.passed_students}")
    print(f"Failed Students: {report.failed_students}")
    print(f"Best Student: {report.best_student}")
    print(f"Worst Student: {report.worst_student}")
    print(f"Average Score: {report.average_score:.2f}")
    
    sparkSession.read.format("delta").load("data/exam_report").show()
    
    sparkSession.stop() 