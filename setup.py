import os
import sys

from setuptools import find_packages, setup

VERSION = "1.0"

setup(
    name="Ginzibix",
    version=VERSION,
    description="A Binary newsreader for the Gnome Desktop",
    author="dermatty",
    author_email="stephan@untergrabner.at",
    url="https://github.com/dermatty/GINZIBIX",
    platforms="posix",
    packages=find_packages(),
    include_package_data=True,
    entry_points={"gui_scripts": ["ginzibix = ginzibix.__main__:run"]},
    install_requires=["ginzyenc>=1.2.3", "pyzmq>=18.0.2", "PyGObject>=3.32.1", "setproctitle>=1.1.10", "peewee>=3.9.6",
                      "pandas>=0.24.2", "rarfile>=3.0", "pexpect>=4.7.0", "inotify-simple>=1.1.8", "lxml>=4.3.4",
                      "psutil>=5.6.", "matplotlib>=3.1.1"],
    keywords=["usenet", "nntp"],
    python_requires=">=3.6.1",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Environment :: X11 Applications :: Gnome",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.6",
    ])
