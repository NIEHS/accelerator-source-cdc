from setuptools import setup, find_packages

setup(
    name="accelerator-source-cdc",
    version="0.1.0",
    description="Ingest and crosswalk for CDC data",
    author="Sue Nolte",
    author_email="sue.nolte@nih.gov",
    url="https://github.com/NIEHS/accelerator-source-cdc",
    packages=find_packages(),
    install_requires=[],
    license="BSD 3-Clause",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9"
)
