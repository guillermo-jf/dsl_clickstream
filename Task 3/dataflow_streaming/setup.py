import setuptools

setuptools.setup(
    name='streaming-pubsub-to-gcs',
    version='1.0',
    install_requires=[],
    packages=setuptools.find_packages(),
)

"""
When you submit your pipeline to a distributed runner like Google Cloud Dataflow, your code needs to be sent to and installed on multiple remote worker machines. The setup.py file is what manages this process. It tells the runner:

What code to package: setuptools.find_packages() finds all your local Python modules (like your main pipeline file and any helper files in the same directory) and bundles them up.
What dependencies to install: The install_requires list tells the worker machines which libraries (e.g., apache-beam[gcp], google-cloud-pubsub) to pip install before starting your pipeline.
"""