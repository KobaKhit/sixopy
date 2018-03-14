from setuptools import setup
# http://python-packaging.readthedocs.io/en/latest/minimal.html#

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='sixopy',
      version='0.1',
      description='Wrapper for pyodbc and tweepy.',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Topic :: Social Content :: Database :: SQL :: BigQuery',
      ],
      keywords='twitter facebook instagram sql api streaming bigquery',
      url='http://github.com/kobakhit/sixopy',
      author='Koba Khitalishvili',
      author_email='kobakhit@gmail.com',
      license='MIT',
      packages=['sixopy'],
      install_requires=[
          'tweepy',
          'instagram-scraper==1.5.9',
          'facepy',
          'google-cloud==0.27',
          'textblob',
          'tqdm'
      ],
      dependency_links=[
        "git+ssh://github.com/rarcega/instagram-scraper.git@1.5.9#egg=instagram-scraper-1.5.9"
      ],
      include_package_data=True,
      zip_safe=False)