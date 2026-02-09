# setup.py

# numpy header not found https://stackoverflow.com/questions/14657375/cython-fatal-error-numpy-arrayobject-h-no-such-file-or-directory
# wasm 
# from wasmpy_build import build_ext
from setuptools import setup, Extension
from setuptools import setup
from Cython.Build import cythonize
from pathlib import Path
from distutils.core import setup, Extension
from Cython.Build import cythonize
import numpy as np




pwd = Path(__file__).parent
filepath = str(pwd/"proxy_core.pyx")

for  root, dirs, files in pwd.walk():
    # print(f"root {root}")
    # print(f"dirs:{dirs}")
    # print(f"files:{files}")
    for file in files:
        # print(f"file:{file}")
        if file.endswith(".pyx"):
            print(f"Compiling Cython file: {root/file}")
            setup(
                ext_modules = cythonize(str(root/file),
                build_dir=str(pwd/"build")),
                include_dirs=[np.get_include()]
                )
            # setup(
            #     ext_modules=cythonize([
            #         Extension(file.split(".")[0], [str(root/file)])
            #         ]),
            #     cmdclass={"build_ext": build_ext},
            #     )
    # setup(ext_modules = cythonize(str(file),build_dir=str(pwd/"build")))
