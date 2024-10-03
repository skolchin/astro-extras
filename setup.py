# Astro SDK Extras project
# (c) kol, 2023-2024
"""
Setup routine for astro_extras package
"""
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.readlines()

setuptools.setup(
    name="astro_extras",
    version="0.1.10.1",
    author="Kol",
    author_email="skolchin@gmail.com",
    description="Additional Astro SDK operators",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/skolchin/astro-extras",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    package_dir={"": "src"},
    packages=['astro_extras', 'astro_extras.hooks', 'astro_extras.operators', 'astro_extras.sensors', 'astro_extras.utils'],
    package_data = {"": ["templates/*.sql"]},
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=requirements,
    setup_requires=['setuptools', 'wheel'],
)
