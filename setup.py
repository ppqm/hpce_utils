#!/usr/bin/env python

from setuptools import find_packages, setup

package="hpce_utils"

def get_version(rel_path):
    """Get version string from package __version__"""
    with open(rel_path) as f:
        for line in f:
            if line.strip().startswith("VERSION"):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


__version__ = get_version(f"src/{package}/version.py")


setup(
    name=package,
    version=__version__,
    maintainer="Jimmy Kromann",
    python_requires=">=3.11",
    packages=find_packages(),
    package_data={"hpce_utils": ["queues/uge/templates/*.jinja"]},
    include_package_data=True,
)
