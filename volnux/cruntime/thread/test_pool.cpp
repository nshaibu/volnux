#include <iostream>
#include <cassert>
#include <atomic>
#include <thread>
#include <chrono>
#include <vector>
#include <pthread.h>
#include <Python.h>
#include "pool.cpp"

// Dummy Python callable for testing
static PyObject* dummy_func(PyObject* self, PyObject* args) {
    int x;
    if (!PyArg_ParseTuple(args, "i", &x)) return NULL;
    return PyLong_FromLong(x * 2);
}

// Dummy Python callable that raises
static PyObject* raise_func(PyObject* self, PyObject* args) {
    PyErr_SetString(PyExc_RuntimeError, "dummy error");
    return NULL;
}

// Helper to create a Task with a Python function and arguments
Task make_task(PyObject* func, int arg, PyObject* future = nullptr) {
    Task t;
    t.func = func;
    Py_INCREF(func);
    t.args = PyTuple_Pack(1, PyLong_FromLong(arg));
    t.kwargs = NULL;
    t.future = future;
    if (future) Py_INCREF(future);
    return t;
}

void test_basic_execution() {
    Py_Initialize();
    PyEval_InitThreads();

    ThreadPool pool(2);

    // Prepare Python callable
    PyMethodDef def = {"dummy_func", dummy_func, METH_VARARGS, "dummy"};
    PyObject* pyfunc = PyCFunction_New(&def, NULL);

    // Prepare a future object
    PyObject* futures_mod = PyImport_ImportModule("concurrent.futures");
    PyObject* future_class = PyObject_GetAttrString(futures_mod, "Future");
    PyObject* future = PyObject_CallObject(future_class, NULL);

    // Submit task
    Task t = make_task(pyfunc, 21, future);
    pool.submit(t);

    // Wait for result
    PyObject* result = NULL;
    for (int i = 0; i < 20; ++i) {
        PyObject* done = PyObject_CallMethod(future, "done", NULL);
        if (PyObject_IsTrue(done)) {
            result = PyObject_CallMethod(future, "result", NULL);
            Py_DECREF(done);
            break;
        }
        Py_DECREF(done);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    assert(result != NULL);
    long val = PyLong_AsLong(result);
    assert(val == 42);

    Py_DECREF(result);
    Py_DECREF(future);
    Py_DECREF(future_class);
    Py_DECREF(futures_mod);
    Py_DECREF(pyfunc);

    pool.stop();
    Py_Finalize();
}

void test_exception_handling() {
    Py_Initialize();
    PyEval_InitThreads();

    ThreadPool pool(1);

    // Prepare Python callable that raises
    PyMethodDef def = {"raise_func", raise_func, METH_VARARGS, "raise"};
    PyObject* pyfunc = PyCFunction_New(&def, NULL);

    PyObject* futures_mod = PyImport_ImportModule("concurrent.futures");
    PyObject* future_class = PyObject_GetAttrString(futures_mod, "Future");
    PyObject* future = PyObject_CallObject(future_class, NULL);

    Task t = make_task(pyfunc, 0, future);
    pool.submit(t);

    // Wait for result
    bool got_exception = false;
    for (int i = 0; i < 20; ++i) {
        PyObject* done = PyObject_CallMethod(future, "done", NULL);
        if (PyObject_IsTrue(done)) {
            PyObject* exc = PyObject_CallMethod(future, "exception", NULL);
            if (exc && exc != Py_None) {
                got_exception = true;
                Py_DECREF(exc);
            }
            Py_XDECREF(exc);
            Py_DECREF(done);
            break;
        }
        Py_DECREF(done);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    assert(got_exception);

    Py_DECREF(future);
    Py_DECREF(future_class);
    Py_DECREF(futures_mod);
    Py_DECREF(pyfunc);

    pool.stop();
    Py_Finalize();
}

void test_multiple_tasks() {
    Py_Initialize();
    PyEval_InitThreads();

    ThreadPool pool(4);

    PyMethodDef def = {"dummy_func", dummy_func, METH_VARARGS, "dummy"};
    PyObject* pyfunc = PyCFunction_New(&def, NULL);

    PyObject* futures_mod = PyImport_ImportModule("concurrent.futures");
    PyObject* future_class = PyObject_GetAttrString(futures_mod, "Future");

    std::vector<PyObject*> futures;
    for (int i = 0; i < 8; ++i) {
        PyObject* future = PyObject_CallObject(future_class, NULL);
        Task t = make_task(pyfunc, i, future);
        pool.submit(t);
        futures.push_back(future);
    }

    for (int i = 0; i < 8; ++i) {
        PyObject* result = NULL;
        for (int j = 0; j < 20; ++j) {
            PyObject* done = PyObject_CallMethod(futures[i], "done", NULL);
            if (PyObject_IsTrue(done)) {
                result = PyObject_CallMethod(futures[i], "result", NULL);
                Py_DECREF(done);
                break;
            }
            Py_DECREF(done);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        assert(result != NULL);
        long val = PyLong_AsLong(result);
        assert(val == i * 2);
        Py_DECREF(result);
        Py_DECREF(futures[i]);
    }

    Py_DECREF(future_class);
    Py_DECREF(futures_mod);
    Py_DECREF(pyfunc);

    pool.stop();
    Py_Finalize();
}

int main() {
    test_basic_execution();
    std::cout << "test_basic_execution passed\n";
    test_exception_handling();
    std::cout << "test_exception_handling passed\n";
    test_multiple_tasks();
    std::cout << "test_multiple_tasks passed\n";
    return 0;
}