from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["beautifulsoup4"]

setup(
    name="cmip6d",
    version="1.0",
    author="Jonathan Quiroz",
    author_email="jonathanqv1@gmail.com",
    description="A package to download NASA NEX/GDDP-CMIP6 downscaled climate change scenarios",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/jonathanqv/cmip6d",
    packages=find_packages(include=['cmip6d']),
    install_requires=requirements,
    license="MIT license",
    license_files = ('LICENSE'),
)
