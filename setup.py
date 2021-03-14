from setuptools import setup

"""
python3 setup.py sdist build
twine upload --repository-url http://localhost:10086 dist/*
pip3 install redis_interface==0.1.0 -i http://localhost:10086/simple/
"""

setup(name='dolls',
      version='0.1.0',
      packages=[
          'dolls',
          'dolls/pydis'
      ])

