#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pthread.h>
#include <queue>
#include <vector>
#include <atomic>


struct Task {
    PyObject* func;
    PyObject* args;
    PyObject* kwargs;
    PyObject* future;
};


class ThreadPool {
private:
    std::vector<pthread_t> threads;
    std::queue<Task> task_queue;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;
    std::atomic<bool> shutdown;
    int num_threads;

    static void* worker_thread(void* arg) {
        ThreadPool* pool = static_cast<ThreadPool*>(arg);

        while (true) {
            Task task;

            // Get task from queue
            pthread_mutex_lock(&pool->queue_mutex);
            while (pool->task_queue.empty() && !pool->shutdown) {
                pthread_cond_wait(&pool->queue_cond, &pool->queue_mutex);
            }

            if (pool->shutdown && pool->task_queue.empty()) {
                pthread_mutex_unlock(&pool->queue_mutex);
                break;
            }

            task = pool->task_queue.front();
            pool->task_queue.pop();
            pthread_mutex_unlock(&pool->queue_mutex);

            // Execute task outside GIL
            PyObject* result = NULL;
            PyObject* exc_type = NULL;
            PyObject* exc_value = NULL;
            PyObject* exc_traceback = NULL;

            // Acquire GIL for Python call
            PyGILState_STATE gstate = PyGILState_Ensure();

            result = PyObject_Call(task.func, task.args, task.kwargs);

            if (result == NULL) {
                // Fetch the exception information
                PyErr_Fetch(&exc_type, &exc_value, &exc_traceback);
                PyErr_NormalizeException(&exc_type, &exc_value, &exc_traceback);

                // If exc_value is NULL, create an instance of the exception type
                if (exc_value == NULL && exc_type != NULL) {
                    exc_value = PyObject_CallObject(exc_type, NULL);
                }
            }

            // Set result or exception on future
            if (task.future) {
                if (result) {
                    PyObject* set_result = PyObject_GetAttrString(task.future, "set_result");
                    if (set_result) {
                        PyObject* ret = PyObject_CallFunctionObjArgs(set_result, result, NULL);
                        Py_XDECREF(ret);
                        Py_DECREF(set_result);
                    }
                } else if (exc_value) {
                    PyObject* set_exception = PyObject_GetAttrString(task.future, "set_exception");
                    if (set_exception) {
                        PyObject* ret = PyObject_CallFunctionObjArgs(set_exception, exc_value, NULL);
                        Py_XDECREF(ret);
                        Py_DECREF(set_exception);
                    }
                }
            }

            // Cleanup
            Py_XDECREF(result);
            Py_XDECREF(exc_type);
            Py_XDECREF(exc_value);
            Py_XDECREF(exc_traceback);
            Py_DECREF(task.func);
            Py_DECREF(task.args);
            Py_XDECREF(task.kwargs);
            Py_XDECREF(task.future);

            // Release GIL
            PyGILState_Release(gstate);
        }

        return NULL;
    }

public:
    ThreadPool(int num_threads) : num_threads(num_threads), shutdown(false) {
        pthread_mutex_init(&queue_mutex, NULL);
        pthread_cond_init(&queue_cond, NULL);
        threads.resize(num_threads);
        for (int i = 0; i < num_threads; i++) {
            pthread_create(&threads[i], NULL, worker_thread, this);
        }
    }

    void submit(Task task) {
        pthread_mutex_lock(&queue_mutex);
        task_queue.push(task);
        pthread_cond_signal(&queue_cond);
        pthread_mutex_unlock(&queue_mutex);
    }

    void stop() {
        shutdown = true;
        pthread_cond_broadcast(&queue_cond);

        for (int i = 0; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
        }

        pthread_mutex_destroy(&queue_mutex);
        pthread_cond_destroy(&queue_cond);
    }

    ~ThreadPool() {
        if (!shutdown) {
            stop();
        }
    }
};


typedef struct {
    PyObject_HEAD
    ThreadPool* pool;
} ThreadPoolExecutorObject;


static PyObject* ThreadPoolExecutor_new(PyTypeObject* type, PyObject* args, PyObject* kwargs) {
    ThreadPoolExecutorObject* self = (ThreadPoolExecutorObject*)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->pool = NULL;
    }
    return (PyObject*)self;
}


static int ThreadPoolExecutor_init(ThreadPoolExecutorObject* self, PyObject* args, PyObject* kwargs) {
    int max_workers = 4;

    static char* kwlist[] = {(char*)"max_workers", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i", kwlist, &max_workers)) {
        return -1;
    }

    if (max_workers <= 0) {
        PyErr_SetString(PyExc_ValueError, "max_workers must be positive");
        return -1;
    }

    self->pool = new ThreadPool(max_workers);
    return 0;
}


static void ThreadPoolExecutor_dealloc(ThreadPoolExecutorObject* self) {
    if (self->pool) {
        self->pool->stop();
        delete self->pool;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject* ThreadPoolExecutor_submit(ThreadPoolExecutorObject* self, PyObject* args, PyObject* kwargs) {
    PyObject* func;
    PyObject* func_args = NULL;
    PyObject* func_kwargs = NULL;

    if (!PyArg_ParseTuple(args, "O|OO", &func, &func_args, &func_kwargs)) {
        return NULL;
    }

    if (!PyCallable_Check(func)) {
        PyErr_SetString(PyExc_TypeError, "First argument must be callable");
        return NULL;
    }

    // Create Future object
    PyObject* concurrent_futures = PyImport_ImportModule("concurrent.futures");
    if (!concurrent_futures) return NULL;

    PyObject* future_class = PyObject_GetAttrString(concurrent_futures, "Future");
    Py_DECREF(concurrent_futures);
    if (!future_class) return NULL;

    PyObject* future = PyObject_CallObject(future_class, NULL);
    Py_DECREF(future_class);
    if (!future) return NULL;

    // Prepare task
    Task task;
    task.func = func;
    Py_INCREF(func);

    if (func_args) {
        task.args = func_args;
        Py_INCREF(func_args);
    } else {
        task.args = PyTuple_New(0);
    }

    if (func_kwargs) {
        task.kwargs = func_kwargs;
        Py_INCREF(func_kwargs);
    } else {
        task.kwargs = NULL;
    }

    task.future = future;
    Py_INCREF(future);

    // Submit to pool (release GIL during submission)
    Py_BEGIN_ALLOW_THREADS
    self->pool->submit(task);
    Py_END_ALLOW_THREADS

    return future;
}


static PyObject* ThreadPoolExecutor_shutdown(ThreadPoolExecutorObject* self, PyObject* args, PyObject* kwargs) {
    int wait = 1;
    static char* kwlist[] = {(char*)"wait", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|p", kwlist, &wait)) {
        return NULL;
    }

    if (self->pool && wait) {
        Py_BEGIN_ALLOW_THREADS
        self->pool->stop();
        Py_END_ALLOW_THREADS
    }

    Py_RETURN_NONE;
}

static PyMethodDef ThreadPoolExecutor_methods[] = {
    {"submit", (PyCFunction)ThreadPoolExecutor_submit, METH_VARARGS, "Submit a callable to be executed"},
    {"shutdown", (PyCFunction)ThreadPoolExecutor_shutdown, METH_VARARGS | METH_KEYWORDS, "Shutdown the executor"},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject ThreadPoolExecutorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "threadpool_ext.ThreadPoolExecutor",  /* tp_name */
    sizeof(ThreadPoolExecutorObject),      /* tp_basicsize */
    0,                                      /* tp_itemsize */
    (destructor)ThreadPoolExecutor_dealloc, /* tp_dealloc */
    0,                                      /* tp_vectorcall_offset */
    0,                                      /* tp_getattr */
    0,                                      /* tp_setattr */
    0,                                      /* tp_as_async */
    0,                                      /* tp_repr */
    0,                                      /* tp_as_number */
    0,                                      /* tp_as_sequence */
    0,                                      /* tp_as_mapping */
    0,                                      /* tp_hash */
    0,                                      /* tp_call */
    0,                                      /* tp_str */
    0,                                      /* tp_getattro */
    0,                                      /* tp_setattro */
    0,                                      /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                    /* tp_flags */
    "ThreadPoolExecutor using pthreads",   /* tp_doc */
    0,                                      /* tp_traverse */
    0,                                      /* tp_clear */
    0,                                      /* tp_richcompare */
    0,                                      /* tp_weaklistoffset */
    0,                                      /* tp_iter */
    0,                                      /* tp_iternext */
    ThreadPoolExecutor_methods,            /* tp_methods */
    0,                                      /* tp_members */
    0,                                      /* tp_getset */
    0,                                      /* tp_base */
    0,                                      /* tp_dict */
    0,                                      /* tp_descr_get */
    0,                                      /* tp_descr_set */
    0,                                      /* tp_dictoffset */
    (initproc)ThreadPoolExecutor_init,     /* tp_init */
    0,                                      /* tp_alloc */
    ThreadPoolExecutor_new,                /* tp_new */
};

// Module definition
static PyModuleDef threadpool_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "threadpool_ext",
    .m_doc = "ThreadPoolExecutor using pthreads and GIL release",
    .m_size = -1,
};

// Module initialization
PyMODINIT_FUNC PyInit_threadpool_ext(void) {
    PyObject* m;

    if (PyType_Ready(&ThreadPoolExecutorType) < 0)
        return NULL;

    m = PyModule_Create(&threadpool_module);
    if (m == NULL)
        return NULL;

    Py_INCREF(&ThreadPoolExecutorType);
    if (PyModule_AddObject(m, "ThreadPoolExecutor", (PyObject*)&ThreadPoolExecutorType) < 0) {
        Py_DECREF(&ThreadPoolExecutorType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}