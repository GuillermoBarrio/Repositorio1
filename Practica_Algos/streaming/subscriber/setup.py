
import setuptools

REQUIRED_PACKAGES = [
    "apache-beam[gcp]==2.24.0",
    "fsspec==0.8.4",
    "gcsfs==0.7.1",
    "loguru==0.5.3",
]

setuptools.setup(
    name="twitchstreaming",
    version="0.0.1",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True,
    description="Twitch Troll Detection",
)