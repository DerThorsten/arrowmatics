import arrowmatics


import pandas as pd
import numpy as np

import pyarrow as pa

from pyarrow.cffi import ffi

NUM_COLUMNS=11
NUM_ROWS=7

df = pd.DataFrame(
    np.random.randint(0, 30, size=(NUM_ROWS, NUM_COLUMNS)),
    columns=[f"COL_{i}" for i in range(NUM_COLUMNS)],
    index=pd.date_range('2000', periods=NUM_ROWS, freq='h'),
    dtype="float32[pyarrow]",
    
)


# Pandas wrappers of `pyarrow.lib.Arrays`
arrow_extension_arrays = df._mgr.arrays

# Likely a list of `pyarrow.lib.FloatArray` (a subclass of `pyarrow.lib.Array`)
# Depending on the dtype, we might have other `pyarrow.lib.*Array` extending `pyarrow.lib.Array`
#
# We need to call `combine_chunks` because `pyarrow.lib.ChunkArray` have (for now) no way for
# exporting or importing data (e.g. with `_import_from_c`/`_export_to_c`).
pyarrow_arrays = list(map(lambda aea: aea._data.combine_chunks(), df._mgr.arrays))
first_pyarrow_array = pyarrow_arrays[0]

print(first_pyarrow_array)

c_schema = ffi.new("struct ArrowSchema*")
c_schema_ptr = int(ffi.cast("uintptr_t", c_schema))

c_array = ffi.new("struct ArrowArray*")
c_array_ptr = int(ffi.cast("uintptr_t", c_array))


# Populate opaque pointers
first_pyarrow_array.type._export_to_c(c_schema_ptr)
first_pyarrow_array._export_to_c(c_array_ptr)


print(c_array_ptr)
print(c_schema_ptr)

arrowmatics.from_py_ptrs(c_schema_ptr, c_array_ptr)