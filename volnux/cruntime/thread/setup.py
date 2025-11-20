import sys

from setuptools import Extension, setup

# Define the extension module
threadpool_ext = Extension(
    "threadpool_ext",
    sources=["pool.cpp"],
    extra_compile_args=["-std=c++11", "-pthread"],
    extra_link_args=["-pthread"],
    language="c++",
)

setup(
    name="threadpool_ext",
    version="1.0",
    description="ThreadPoolExecutor using pthreads with GIL release",
    ext_modules=[threadpool_ext],
    zip_safe=False,
)
