"""Beam - MySQL Connector for setup file."""

import os
import subprocess

from setuptools import Command, find_packages, setup

SRC_DIR_NAME = "beam_mysql"
EXAMPLES_DIR_NAME = "examples"
TESTS_DIR_NAME = "tests"


class SimpleCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class LintCommand(SimpleCommand):
    def run(self):
        subprocess.run(
            [
                "black",
                SRC_DIR_NAME,
                EXAMPLES_DIR_NAME,
                TESTS_DIR_NAME,
                "-l",
                "120",
                "--check",
                "--diff",
            ]
        )
        subprocess.run(["mypy", SRC_DIR_NAME, EXAMPLES_DIR_NAME, TESTS_DIR_NAME])


class FormatCommand(SimpleCommand):
    def run(self):
        subprocess.run(
            ["black", SRC_DIR_NAME, EXAMPLES_DIR_NAME, TESTS_DIR_NAME, "-l", "120"]
        )


def get_version():
    global_names = {}
    exec(
        open(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "beam_mysql/__init__.py"
            )
        ).read(),
        global_names,
    )
    return global_names["__version__"]


with open("README.md") as f:
    README = f.read()


PACKAGE_NAME = "beam-mysql-connector"
PACKAGE_VERSION = get_version()
PACKAGE_DESCRIPTION = "Apache Beam I/O connector designed for accessing MySQL databases."
PACKAGE_URL = "https://github.com/esakik/beam-mysql-connector"
PACKAGE_DOWNLOAD_URL = "https://pypi.python.org/pypi/beam-mysql-connector"
PACKAGE_AUTHOR = "esakik"
PACKAGE_EMAIL = "esakik.dev@gmail.com"
PACKAGE_KEYWORDS = "apache beam mysql connector"
PACKAGE_LONG_DESCRIPTION = README

REQUIRED_PACKAGES = [
    "apache-beam>=2.25.0",
    "mysql-connector-python>=8.1.0",
]

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description=PACKAGE_DESCRIPTION,
    long_description=PACKAGE_LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url=PACKAGE_URL,
    download_url=PACKAGE_DOWNLOAD_URL,
    author=PACKAGE_AUTHOR,
    author_email=PACKAGE_EMAIL,
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
    python_requires=">=3.8,<3.12",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    license="MIT",
    keywords=PACKAGE_KEYWORDS,
    cmdclass=dict(lint=LintCommand, fmt=FormatCommand),
)
