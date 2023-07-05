import pyarrow as pa
from pyarrow.cffi import ffi
import arrowmatics


import pyarrow as pa
n_legs = pa.array([2, 2, 1])
weight = pa.array([1.0, 2.0, 3.5], type=pa.float32())
animals = pa.array(["Flamingo", "Parrot", "stick"])
batch = pa.RecordBatch.from_arrays([n_legs, weight],
                                    names=["n_legs","stick"])

def arrow_array_to_c_schema_array_ptr_pair(array):
    c_schema = ffi.new("struct ArrowSchema*")
    c_schema_ptr = int(ffi.cast("uintptr_t", c_schema))

    c_array = ffi.new("struct ArrowArray*")
    c_array_ptr = int(ffi.cast("uintptr_t", c_array))


    array.type._export_to_c(c_schema_ptr)
    array._export_to_c(c_array_ptr)

    return c_schema_ptr, c_array_ptr


cs,ca = arrow_array_to_c_schema_array_ptr_pair(batch.column(0))

#arrowmatics.print_double_array(cs,ca)

native_tensor = arrowmatics.as_native_tensor(cs,ca)



arr = native_tensor.as_numpy()
print(arr)

arr[0] = 100

print(n_legs.to_numpy())