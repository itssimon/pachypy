from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='pachypy',
    version='0.1.2',
    author='Simon Gurcke',
    description='Python client library for Pachyderm',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/itssimon/pachypy',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3 :: Only',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
    ],
    install_requires=[
        'python_pachyderm>=1.8.6',
        'docker>=3.7.2',
        'boto3>=1.9.119',
        'pandas>=0.24.2',
        'pyyaml>=3.13',
        'tzlocal>=1.5.1',
        'termcolor>=1.1.0',
        'ipython>=7.4.0',
    ],
    extras_require={
        'docs':  [
            'sphinx>=2.0.0',
            'sphinx_rtd_theme>=0.4.3',
            'sphinx_autodoc_typehints>=1.6.0',
        ],
        'test': [
            'pytest>=4.3.1',
            'pytest-cov>=2.6.1',
        ]
    }
)
