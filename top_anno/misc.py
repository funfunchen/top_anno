import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import numpy as np
import pandas as pd

class Anno_file():
    def __init__(self, ref_path, chrom, anno_type):
        self._ref_file = ref_path + "/" + "freeze.6.chr" + chrom + "." + anno_type + ".general"
        self.anno_col_names = pd.read_csv(self._ref_file, sep="\t", nrows=1).columns.tolist()
        self.dtype_tuples = [(x, str) for x in anno_col_names]
        self.dtypes = dict(dtype_tuples)

    def dd_df():
        df = dd.read_csv(self._ref_file, sep="\t", dtype=self.dtypes)
        return df


class Res_file():
    def __init__(self, res_path):
        self._res = res_path
        self.col_names = ['chr', 'pos', 'REF', 'ALT', 'af', 'stat', 'p_gscan', 'beta', 'se',
                    'N', 'direction', 'effective_N', 'U1', 'U2', 'p_trans', 'U4', 'U5']
        d_tuples = [(x, float) for x in self.col_names]
        d_types = dict(d_tuples)
        self._d_types = dtypes.update({'pos': int, 'chr': str, 'direction': str, 'REF': str, 'ALT':str, 'pos':int})

    def dd_df():
        df = dd.read_csv(self._res,  sep="\t", names=self.col_names, dtype=self._d_types)
        return df


class simple_logger(object):
    def __init__(self, logfile=None, verbose=True):
        self.console = sys.stdout
        self.verbose = verbose
        if logfile is not None:
            self.log = open(logfile, 'w')
        else:
            self.log = None

    def write(self, message):
        if self.verbose:
            self.console.write(message+'\n')
        if self.log is not None:
            self.log.write(message+'\n')
            self.log.flush()
