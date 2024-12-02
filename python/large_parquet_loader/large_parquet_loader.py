import os
from glob import glob
from itertools import product

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

class ParquetReader:
    """Filter and load a parquet by finding all of the relevent paths.

    Args:
        base_dir - The top-level directory that contains all the Parquet data
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def load_parquet(self, partitions: dict):
        """Filter and load a parquet by finding all of the relevent paths.

        Args:
            partitions - A dictionary where the keys are the Parquet partitions (in order) and the values are the specific partitions that should be read in.
        """
        partition_values = [x if hasattr(x, "__iter__") else [x] for x in partitions.values()]  # Convert to iterable
        paths = []
        for combo in list(product(*partition_values)):
            # Construct list of path elements with wildcard at end
            parquet_path = [f"{pp}={combo[i]}" for i, pp in enumerate(partitions.keys())]
            total_path = [self.base_dir] + parquet_path + ["*"]

            # Find any files that match this pattern
            paths.append(glob(os.path.join(*total_path)))
        flattened_paths = [x for lst in paths for x in lst]

        partitioning = ds.HivePartitioning(pa.schema([(p, pa.string()) for p in partitions.keys()]))
        df = pq.ParquetDataset(flattened_paths, partitioning=partitioning).read().to_pandas()
        return df
