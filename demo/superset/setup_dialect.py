from setuptools import setup

setup(
    name="elastiq-dialect",
    version="0.1.0",
    py_modules=["elastic_dialect"],
    entry_points={
        "sqlalchemy.dialects": [
            "elastiq = elastic_dialect:ElastiqDialect",
        ],
    },
    install_requires=["flightsql-dbapi"],
)
