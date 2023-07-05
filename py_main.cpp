#include <pybind11/pybind11.h>
#include <iostream>
#include <pybind11/numpy.h>

#include "nanoarrow.hpp"
namespace arcticdbproxy { 

    enum class DataType : uint8_t {
        UINT8=0,
        UINT16=1,
        UINT32=2,
        UINT64=3,
        INT8=4,
        INT16=5,
        INT32=6,
        INT64=7,
        FLOAT32=8,
        FLOAT64=9,
        BOOL8=10,
        MICROS_UTC64=11,
        ASCII_FIXED64=12,
        ASCII_DYNAMIC64=13,
        UTF_FIXED64=14,
        UTF_DYNAMIC64=15,
        BYTES_DYNAMIC64=16,
        UNKNOW=17
    };


    struct NativeTensor{

        ssize_t nbytes_;
        ssize_t ndim_;
        std::vector<std::size_t> strides_ = {};
        std::vector<std::size_t> shapes_ = {};
        DataType dt_;
        ssize_t elsize_;
        const void *ptr;
        bool owns_memory;
    };
}



int print_double_array(
    uint64_t  schema_ptr_int,
    uint64_t  arraw_ptr_int
)
{
    // change the adress of the pointer to the value of the interger
    ArrowSchema *schema = reinterpret_cast<ArrowSchema *>(schema_ptr_int);
    ArrowArray * array = reinterpret_cast<ArrowArray *>(arraw_ptr_int);

    // inspect the schema
    std::cout << "schema_ptr->format: " << schema->format << std::endl;


    ArrowError error;
    ArrowArrayView array_view;

    NANOARROW_RETURN_NOT_OK(ArrowArrayViewInitFromSchema(&array_view, schema, &error));

    if (array_view.storage_type != NANOARROW_TYPE_DOUBLE) {
        printf("Array has storage that is not double\n");
    }

    int result = ArrowArrayViewSetArray(&array_view, array, &error);
    if (result != NANOARROW_OK) {
        ArrowArrayViewReset(&array_view);
        return result;
    }

    ArrowBufferView* data_view = &array_view.buffer_views[1];
    const double * float_ptr = data_view->data.as_double + array_view.offset;


    for (int64_t i = 0; i < array->length; i++) {
        std::cout<<float_ptr[i]<<"\n";
    }

    ArrowArrayViewReset(&array_view);

    return 0;

}



arcticdbproxy::NativeTensor as_native_tensor(
    uint64_t  schema_ptr_int,
    uint64_t  arraw_ptr_int
){
    // convert ptrs held as integers to actual arrow types
    ArrowSchema *schema = reinterpret_cast<ArrowSchema *>(schema_ptr_int);
    ArrowArray * array = reinterpret_cast<ArrowArray *>(arraw_ptr_int);

    // we do not allow nested schemas
    if(schema->n_children != 0){
        throw std::runtime_error("schema has children");
    }

    ArrowError error;
    ArrowArrayView array_view;
   
    if(ArrowArrayViewInitFromSchema(&array_view, schema, &error) != NANOARROW_OK){
        throw std::runtime_error("error in ArrowArrayViewInitFromSchema" + std::string(error.message));
    }

    if (ArrowArrayViewSetArray(&array_view, array, &error) != NANOARROW_OK) {
        throw std::runtime_error("error in ArrowArrayViewSetArray" + std::string(error.message));
    }

    // create a native tensor object
    arcticdbproxy::NativeTensor native_tensor;

    // std::cout << "schema_ptr->format: " << schema->format << std::endl;

    // std::cout<<"print"<<std::endl;
    // std::cout<< ArrowArrayViewGetDoubleUnsafe(&array_view, 0)<<std::endl;
    // std::cout<< ArrowArrayViewGetDoubleUnsafe(&array_view, 1)<<std::endl;
    // std::cout<< ArrowArrayViewGetDoubleUnsafe(&array_view, 2)<<std::endl;

    ArrowBufferView* data_view = &array_view.buffer_views[1];

    switch(array_view.storage_type){
        case NANOARROW_TYPE_UINT8:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::UINT8;
            native_tensor.elsize_ = 1;
            native_tensor.ptr = data_view->data.as_uint8 + array_view.offset;
            break;
        }
        case NANOARROW_TYPE_UINT16:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::UINT16;
            native_tensor.elsize_ = 2;
            native_tensor.ptr = data_view->data.as_uint16 + array_view.offset;
            break;
        }
        case NANOARROW_TYPE_UINT32:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::UINT32;
            native_tensor.ptr = data_view->data.as_uint32 + array_view.offset;
            native_tensor.elsize_ = 4;
            break;
        }
        case NANOARROW_TYPE_UINT64:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::UINT64;
            native_tensor.ptr = data_view->data.as_uint64 + array_view.offset;
            native_tensor.elsize_ = 8;
            break;
        }
        case NANOARROW_TYPE_INT8:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::INT8;
            native_tensor.ptr = data_view->data.as_int8 + array_view.offset;
            native_tensor.elsize_ = 1;
            break;
        }
        case NANOARROW_TYPE_INT16:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::INT16;
            native_tensor.ptr = data_view->data.as_int16 + array_view.offset;
            native_tensor.elsize_ = 2;
            break;
        }
        case NANOARROW_TYPE_INT32:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::INT32;
            native_tensor.ptr = data_view->data.as_int32 + array_view.offset;
            native_tensor.elsize_ = 4;
            break;
        }
        case NANOARROW_TYPE_INT64:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::INT64;
            native_tensor.ptr = data_view->data.as_int64 + array_view.offset;
            native_tensor.elsize_ = 8;
            break;
        }
        case NANOARROW_TYPE_FLOAT:
        {    
            native_tensor.dt_ = arcticdbproxy::DataType::FLOAT32;
            native_tensor.ptr = data_view->data.as_float + array_view.offset;
            native_tensor.elsize_ = 4;
            break;
        }
        case NANOARROW_TYPE_DOUBLE:
        {    
            std::cout<<"here array_view.offset"<<array_view.offset<<"\n";
            const double * ptr = data_view->data.as_double + array_view.offset;
            std::cout<<"ptr[0]"<<ptr[0]<<"\n";
            std::cout<<"ptr[1]"<<ptr[1]<<"\n";
            std::cout<<"ptr[2]"<<ptr[2]<<"\n";
            native_tensor.dt_ = arcticdbproxy::DataType::FLOAT64;
            native_tensor.ptr = data_view->data.as_double + array_view.offset;
            native_tensor.elsize_ = 8;
            break;
        }
        default:
        {    
            throw std::runtime_error(std::string("unsupported arrow type: ") + ArrowTypeString(array_view.storage_type));
        }
    }
    //std::cout<<"dt"<<int(uint8_t(native_tensor.dt_))<<"\n";
    native_tensor.ndim_ = 1;
    native_tensor.nbytes_ = native_tensor.elsize_ * array->length;
    native_tensor.strides_ = {std::size_t(native_tensor.elsize_)};
    native_tensor.shapes_ = {std::size_t(array->length)};

    return native_tensor;
}






namespace py = pybind11;
PYBIND11_MODULE(_arrowmatics, m) {
    m.doc() = "_arrowmatics";

    py::class_<arcticdbproxy::NativeTensor>(m, "NativeTensor") 
        .def(py::init<>())
        .def("as_numpy", [](arcticdbproxy::NativeTensor &self) -> py::object {
            
            void * ptr = const_cast<void*>(self.ptr);
            if(self.dt_ == arcticdbproxy::DataType::UINT8)
            {
                return py::array_t<uint8_t>(self.shapes_, self.strides_, static_cast<uint8_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::UINT16) 
            {
                return py::array_t<uint16_t>(self.shapes_, self.strides_, static_cast<uint16_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::UINT32) 
            {
                return py::array_t<uint32_t>(self.shapes_, self.strides_, static_cast<uint32_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::UINT64) 
            {
                return py::array_t<uint64_t>(self.shapes_, self.strides_, static_cast<uint64_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::INT8) 
            {
                return py::array_t<int8_t>(self.shapes_, self.strides_, static_cast<int8_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::INT16) 
            {
                return py::array_t<int16_t>(self.shapes_, self.strides_, static_cast<int16_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::INT32) 
            {
                return py::array_t<int32_t>(self.shapes_, self.strides_, static_cast<int32_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::INT64) 
            {
                return py::array_t<int64_t>(self.shapes_, self.strides_, static_cast<int64_t*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::FLOAT32) 
            {
                return py::array_t<float>(self.shapes_, self.strides_, static_cast<float*>(ptr));
            }
            else if(self.dt_ == arcticdbproxy::DataType::FLOAT64) 
            {
                return py::array_t<double>(self.shapes_, self.strides_, static_cast<double*>(ptr));
            }
            else{
                throw py::type_error("unsupported type");
            }
            
        })
        ;

    m.def("as_native_tensor", &as_native_tensor, "as_native_tensor");
    m.def("print_double_array", &print_double_array, "print_double_array");
}