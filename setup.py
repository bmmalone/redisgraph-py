from setuptools import setup, find_packages
import io

def read_all(f):
    with io.open(f, encoding="utf-8") as I:
        return I.read()


requirements = list(map(str.strip, open("requirements.txt").readlines()))


tests_require = [
    'pytest',
    'coverage',
    'pytest-cov',
    'coveralls',
    'pytest-runner',
]

docs_require = [
    'sphinx',
    'sphinx_rtd_theme',
]

all_requires = (
    tests_require
    + docs_require
)

extras = {
    'test': tests_require,
    'all': all_requires,
    'docs': docs_require,
}

setup(
    name='redisgraph',
    version='3.0.0',
    description='RedisGraph Python Client',
    long_description=read_all("README.md"),
    long_description_content_type='text/markdown',
    url='https://github.com/bmmalone/redisgraph-py',
    packages=find_packages(),
    install_requires=requirements,
    extras_require=extras,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database'
    ],
    keywords='Redis Graph Extension',
    author='Brandon Malone',
    author_email='bmmalone@gmail.com'
)
