from setuptools import find_packages, setup


setup(
    name="sparkos",
    version="0.1.0",
    description="A natural-language TUI workbench for large-scale data and graph analysis.",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.9",
    install_requires=[
        "pydantic>=2.7",
        "pyyaml>=6.0",
        "textual>=0.85",
    ],
    extras_require={
        "spark": ["pyspark>=3.5"],
        "dev": ["pytest>=8.0"],
    },
    entry_points={
        "console_scripts": [
            "sparkos=sparkos.cli:main",
        ],
    },
)
