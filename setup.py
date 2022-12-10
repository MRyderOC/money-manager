import pathlib
from setuptools import setup, find_packages

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")
source_url = "https://github.com/MRyderOC/money-manager"

setup(
    name="money-manager",
    version="0.0.2",
    description="A package to manage your money.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=source_url,
    author="Milad Tabrizi",
    author_email="milad@miladtabrizi.com",
    license="GPLv3+",
    package_dir={"":"src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    python_requires=">=3.7, <4",

    # PyPA Optionals
    keywords="finance, expense, money",
    project_urls={
        "Bug Reports": "https://github.com/MRyderOC/money-manager/issues",
        "Source": source_url,
    },
    # classifiers=[],
)
