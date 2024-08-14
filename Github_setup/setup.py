from setuptools import setup, find_packages

setup(
    name='my_package',  # Replace with your package name
    version='0.1.1',
    packages=find_packages(),
    install_requires=[
        'boto3==1.26.142',
        'great-expectations==0.17.9',
        'pymysql==1.0.2',
        'pyspark==3.3.2',
        'pyyaml==6.0',
        'slack-sdk==3.19.3',
        'sqlalchemy==1.4.39',
    ],
    author='Vivek',
    author_email='Vivektatikonda3@gmail.com',
    description='Custom Great Expectation Package',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/vivektatikonda3/Great-Expectation',  # Replace with your repository URL
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)