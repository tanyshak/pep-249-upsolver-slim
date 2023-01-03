"""Setup script for PEP249."""
import setuptools
from pep249 import __version__

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="pep249upsolver",
    version=__version__,
    author="Tetiana Shemet",
    author_email="tanyshak@gmail.com",
    url="https://github.com/tanyshak/pep-249-upsolver-slim",
    description="Slim implementation of the DB 2.0 API outlined in PEP-249. for upsolver",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Development Status :: 4 - Beta",
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: MIT License",
        "Typing :: Typed",
    ],
    python_requires=">=3.7",
)
