#include <pybind11/pybind11.h>
#include <iostream>

#include "nanoarrow.hpp"



int print_float_array(
    uint64_t  schema_ptr_int,
    uint64_t  arraw_ptr_int
)
{
    // change the adress of the pointer to the value of the interger
    ArrowSchema *schema = reinterpret_cast<ArrowSchema *>(schema_ptr_int);
    ArrowArray * array = reinterpret_cast<ArrowArray *>(arraw_ptr_int);

    // inspect the schema
    std::cout << "schema_ptr->format: " << schema->format << std::endl;


    struct ArrowError error;
    struct ArrowArrayView array_view;

    NANOARROW_RETURN_NOT_OK(ArrowArrayViewInitFromSchema(&array_view, schema, &error));

    if (array_view.storage_type != NANOARROW_TYPE_FLOAT) {
        printf("Array has storage that is not float\n");
    }

    int result = ArrowArrayViewSetArray(&array_view, array, &error);
    if (result != NANOARROW_OK) {
        ArrowArrayViewReset(&array_view);
        return result;
    }

    struct ArrowBufferView* data_view = &array_view.buffer_views[1];
    const float * float_ptr = data_view->data.as_float + array_view.offset;


    for (int64_t i = 0; i < array->length; i++) {
        std::cout<<float_ptr[i]<<"\n";
    }

    ArrowArrayViewReset(&array_view);

    return 0;

}






namespace py = pybind11;
PYBIND11_MODULE(_arrowmatics, m) {
    m.doc() = "_arrowmatics";

    m.def("print_float_array", &print_float_array, "print_float_array");
}