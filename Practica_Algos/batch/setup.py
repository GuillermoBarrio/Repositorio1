
import setuptools

REQUIRED_PACKAGES = [
    "apache-beam[gcp]==2.24.0",
    "tensorflow-transform==0.24.1",
    "tensorflow==2.3.0",
    "tfx==0.24.1",
    "gensim==3.6.0",
    "fsspec==0.8.4",
    "gcsfs==0.7.1",
]

setuptools.setup(
    name="twitchstreaming",
    version="0.0.1",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True,
    description="Troll detection",
)