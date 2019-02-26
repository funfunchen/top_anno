import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import numpy as np
import pandas as pd
import argparse
import time

parser = argparse.ArgumentParser(description='Top_anno: DASK based annotator for the Topmed project')
parser.add_argument('--ref_db', type=str, help='path for the refs')
parser.add_argument('--res', type=str, help='path for the results to be annotated')
parser.add_argument('--ch', type=str, help='annotate chromosome')
parser.add_argument('--anno_type', default='snp', choices=['snp', 'indel', 'tissue-specific'], help='annotation type'
args = parser.parse_args()

anno_file = Anno_file(args.ref_db, args.ch, args.anno_type)
res_file = Res_file(args.res)


