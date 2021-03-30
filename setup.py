from setuptools import setup

"""
python3 setup.py sdist build
twine upload --repository-url http://localhost:10086 dist/*
pip3 install redis_interface==0.1.0 -i http://localhost:10086/simple/
"""

from setuptools import setup, find_packages
import io

def read_all(f):
    with io.open(f, encoding="utf-8") as I:
        return I.read()

requirements = list(map(str.strip, open("requirements.txt").readlines()))

setup(
    name='dolls',
    version='1.0.0',
    description='python 工具包',
    long_description=read_all("README.md"),
    long_description_content_type='text/markdown',
    url='http://github.com/zzq120203/dolls-python',
    packages=find_packages(),
    install_requires=requirements,
    author='Zhang Zhanqi',
    author_email='zzq120203@163.com'
)