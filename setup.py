#!/usr/bin/env python3
"""CaproneISIS package setup."""

from setuptools import setup, find_packages
from pathlib import Path

readme = Path(__file__).parent / "README.md"
long_description = readme.read_text(encoding="utf-8") if readme.exists() else ""

setup(
    name="caproneisis",
    version="0.1.0",
    author="Caprazli",
    author_email="caprazli@protonmail.com",
    description="Industrial-scale CDS/ISIS implementation using Elasticsearch",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/capraCoder/caproneisis",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Database",
        "Topic :: Text Processing :: Indexing",
    ],
    python_requires=">=3.9",
    install_requires=[
        "elasticsearch>=8.0.0",
    ],
    extras_require={
        "dev": ["pytest", "pytest-cov"],
    },
    entry_points={
        "console_scripts": [
            "caproneisis=caproneisis.cli:main",
        ],
    },
)
