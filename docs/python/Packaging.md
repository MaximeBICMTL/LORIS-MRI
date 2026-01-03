# LORIS Python packaging

## Modern packaging

LORIS Python is comprised of a Python package that can be installed using standard Python packaging tools such as PIP. To do, use the command `pip install .` in the LORIS-MRI directory. Alternatively, LORIS Python can be installed directly from GitHub using the command `pip install git+https://github.com/aces/loris-mri`.

While `pip install` installs the scripts and libraries locally, note that it does not handle the project environment, variables, and configuration files. As such, it is recommended to use the installation script to install LORIS-MRI.

## Legacy packaging

TODO:

- LIKELY NOT TRANSITIVE

- Use `$PYTHONPATH`

variables: first has precedecence
```
python/blablabla
project/blablabla
```

How about scripts, maybe install all first and then delete libraries but keep scripts?
