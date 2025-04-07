from setuptools import setup, find_packages

setup(
    name="exam_processor",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.5.5",
        "delta-spark==3.3.0",
        "pandas>=1.3.0",
        "pyarrow>=6.0.0",
        "pydantic>=2.0.0",
        "pytest>=7.0.0"
    ],
) 