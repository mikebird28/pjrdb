 #!/usr/bin/env python

from distutils.core import setup

requires = []

setup(name='pjrdb',
    version='0.1.0',
    description='Command-line tools to get formed pjrdb dataset.',
    author='Keisuke Miura',
    author_email='hello.mikeneko@gmail.com',
    url = 'https://mikebird28.hatenablog.jp/',
    licence = 'MIT',
    packages = ["pjrdb"],
    install_requires=requires,
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
    ],
    entry_points = {
        'console_scripts': ['pjrdb=pjrdb.main:main'],
    }
)
