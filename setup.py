#!/usr/bin/env python
#-*- coding:utf-8 -*-

#############################################
# File Name: setup.py
# Author: mage
# Mail: mage@woodcol.com
# Created Time:  2018-1-23 19:17:34
#############################################


from setuptools import setup, find_packages

setup(
    name="rtian_tools",
    version="0.1.0",
    keywords=("pip", ),
    description="time and path tool",
    long_description="time and path tool",
    license ="MIT Licence",

    url="https://github.com/tr862316121/fengjr-research-spider",
    author="rtian",
    author_email="862316121@qq.com",

    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=[]
)
