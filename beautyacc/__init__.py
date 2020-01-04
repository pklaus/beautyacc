from beautyacc.core import CoreArchive
from beautyacc.pandas import PandasArchive
from beautyacc.numpy import NumpyArchive

class Archive(PandasArchive, NumpyArchive, CoreArchive):
    pass
