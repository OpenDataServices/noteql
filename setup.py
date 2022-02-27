#!/usr/bin/env python

from distutils.core import setup


setup(
    name="noteql",
    version="0.7.1",
    description="Write sql in a notbook",
    author="David Raznick",
    author_email="david.raznick@opendataservices.coop",
    url="https://github.com/OpenDataServices/noteql",
    packages=["noteql"],
    install_requires=[
        "ijson",
        "xmltodict",
        "sqlalchemy",
        "psycopg2-binary",
        "ijson",
        "click",
        "jupyter",
        "jinja2",
        "lxml",
        "requests",
        "jinjasql",
        "pandas",
        "openpyxl",
    ],
)
