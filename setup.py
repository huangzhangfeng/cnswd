from setuptools import setup, find_packages
from os import path
import io

here = path.abspath(path.dirname(__file__))

with io.open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requires = f.read().split()

setup(
    name="cnswd",
    version="2.0.0",
    packages=find_packages(),
    long_description="""
    本包使用Firefox浏览器，必须安装Firefox driver。\n
    安装方法参考 https://askubuntu.com/questions/870530/how-to-install-geckodriver-in-ubuntu
    """,
    install_requires=requires +
    ['python_version>="3.7"'],
    tests_require=["pytest", "parameterized"],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'stock = cnswd.scripts.command:stock',
        ],
    },
    author="LDF",
    author_email="liu.dengfeng@hotmail.com",
    description="Utilities for fetching Chinese stock webpage data",
    license="https://github.com/liudengfeng/cnswd/blob/master/LICENSE",
    keywords="Chinese stock data tools",
)
