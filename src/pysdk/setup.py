#!/usr/bin/env python

import os

import setuptools
import shutil
import platform

# make the diongsdk python package dir
shutil.rmtree("dingosdk", ignore_errors=True)
os.mkdir("dingosdk")
shutil.copyfile("__init__.py", "dingosdk/__init__.py")
shutil.copyfile("loader.py", "dingosdk/loader.py")
shutil.copyfile("dingosdk.py", "dingosdk/dingosdk.py")
ext = ".pyd" if platform.system() == "Windows" else ".so"
dingosdk_lib = f"_dingosdk{ext}"
shutil.copyfile(dingosdk_lib, f"dingosdk/_dingosdk{ext}")

long_description = """
dingosdk is dingo-store python sdk for efficient interact with the dingo-store cluster. 
It contains RawKV, Txn,  Vector Api.
NOTE: only support LINUX now.
"""
setuptools.setup(
    name="dingosdk",
    version="0.1rc10-13",
    description="dingo-store python sdk",
    license="Apache License 2.0",
    long_description=long_description,
    url="https://www.dingodb.com/",
    project_urls={
        "Homepage": "https://www.dingodb.com/",
        "Documentation": "https://dingodb.readthedocs.io/en/latest/",
    },
    author="DingoDB",
    author_email="dingodb@zetyun.com",
    keywords="dingodb dingo-store",
    python_requires=">=3.7",
    packages=["dingosdk"],
    package_data={
        "dingosdk": ["*.so", "*.pyd"],
    },
    zip_safe=False,
)
