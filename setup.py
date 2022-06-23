"""
python3 setup.py sdist build
python3 setup.py bdist_wheel
twine upload --repository-url http://localhost:10086 dist/*
pip3 install redis_interface==0.1.0 -i http://localhost:10086/simple/
"""

import io

from setuptools import setup, find_packages


def read_all(f):
    with io.open(f, encoding="utf-8") as I:
        return I.read()


requirements = list(map(str.strip, open("requirements.txt").readlines()))

setup(
    name='dolls',
    version='1.1.2',
    description='python tools',
    long_description=read_all("README.md"),
    long_description_content_type='text/markdown',
    url='https://github.com/zzq120203/dolls-python',
    packages=find_packages(),
    install_requires=requirements,
    author='zhangzhanqi',
    author_email='zzq120203@163.com'
)
