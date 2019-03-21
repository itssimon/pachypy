from setuptools import setup, find_packages

setup(
    name='pachypy',
    version='0.1.1',
    author='Simon Gurcke',
    packages=find_packages(),
    install_requires=[
        'pandas', 'pyyaml', 'tzlocal', 'termcolor',
        'python_pachyderm', 'python-dxf', 'boto3'
    ],
    extras_require={
        'docs':  ['sphinx_rtd_theme', 'sphinx_autodoc_typehints'],
        'test': ['pytest', 'pytest-cov']
    }
)
