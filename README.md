# arrowmatics
nicely seasoned arrowmatics


* tiny pybind11 python module defined in `py_main.cpp`
* the CMakeList is st. such that the build python extension dynamic library is placed in the `arrowmatics` module folder
* the `experiment.py` scipt imports the `arrowmatics` module and uses it to pass a arrow float arraw (and the schema)
  to C++ via the `arrowmatics` module

