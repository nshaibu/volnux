# Python Code Style Guide

## Overview

This document defines the coding standards for our Python project. All contributors must adhere to these guidelines to ensure code quality, maintainability, and consistency across the codebase.

## Code Formatting

### Black Formatter

All Python code must be formatted using [Black](https://black.readthedocs.io/) with default settings.

```bash
# Format a single file
black your_file.py

# Format entire project
black .

# Check formatting without modifying files
black --check .
```

**Configuration**: Black uses its default configuration (88 character line length). Do not override these defaults.

## Type Annotations

### Static Type Checking

We use [mypy](http://mypy-lang.org/) for static type analysis. All code must pass mypy checks with strict mode enabled.

```bash
# Run mypy on the project
mypy .
```

### Type Annotation Requirements

**All functions and methods must have complete type annotations**, including:

- All parameters
- Return types
- Class attributes
- Module-level variables (when appropriate)

#### Examples

```python
from typing import List, Dict, Optional, Tuple

def process_data(items: List[str], threshold: int = 10) -> Dict[str, int]:
    """Process items and return frequency counts."""
    ...

class DataProcessor:
    cache_size: int
    
    def __init__(self, cache_size: int = 100) -> None:
        """Initialize the data processor."""
        self.cache_size = cache_size
    
    async def fetch_data(self, url: str, timeout: Optional[float] = None) -> bytes:
        """Fetch data from the given URL."""
        ...
```

### Type Hints Best Practices

- Use `typing` module types for complex structures
- Use `Optional[T]` for values that can be `None`
- Use `Union[T1, T2]` sparingly; prefer more specific types
- Use `typing.Protocol` for structural subtyping
- Use `typing.TypeVar` for generic types

## Documentation

### Docstring Requirements

**Every function and method must have a docstring** following the format below.

#### Docstring Format

```python
def function_name(param1: str, param2: int) -> Tuple[List[str], List[Exception]]:
    """Brief one-line summary of what the function does.
    
    Optional longer description providing more context about the function's
    behavior, algorithms used, or important implementation details.
    
    Args:
        param1: Description of first parameter
        param2: Description of second parameter
    
    Returns:
        Description of return value
    
    Raises:
        SpecificException: When this specific error occurs
        AnotherException: When this other error occurs
        Exception: For any other critical failures
    """
```

#### Docstring Sections

1. **Summary** (Required): One-line description
2. **Description** (Optional): Extended explanation if needed
3. **Args** (Required if function has parameters): List all parameters
4. **Returns** (Required if function returns a value): Describe the return value
5. **Raises** (Required if function can raise exceptions): List all exceptions

#### Complete Example

```python
from typing import Tuple, List

async def execute_tasks(
    tasks: List[Task],
    timeout: float = 30.0,
    max_retries: int = 3
) -> Tuple[List[Result], List[Exception]]:
    """Execute the tasks asynchronously.
    
    This function processes a list of tasks concurrently and collects
    both successful results and any errors that occurred during execution.
    
    Args:
        tasks: List of Task objects to execute
        timeout: Maximum execution time in seconds
        max_retries: Number of retry attempts for failed tasks
    
    Returns:
        Tuple of (results, errors) from task execution
    
    Raises:
        ExecutionTimeoutError: If execution exceeds timeout
        ExecutionError: If execution fails due to runtime errors
        Exception: If execution fails critically
    """
    ...
```

### Class Docstrings

Classes must also include docstrings:

```python
class TaskExecutor:
    """Manages asynchronous task execution with retry logic.
    
    This class provides a robust task execution framework with support
    for timeouts, retries, and error handling.
    
    Attributes:
        max_workers: Maximum number of concurrent workers
        default_timeout: Default timeout for task execution
    """
    
    max_workers: int
    default_timeout: float
    
    def __init__(self, max_workers: int = 10, default_timeout: float = 30.0) -> None:
        """Initialize the task executor.
        
        Args:
            max_workers: Maximum number of concurrent workers
            default_timeout: Default timeout in seconds
        """
        self.max_workers = max_workers
        self.default_timeout = default_timeout
```

### Module Docstrings

Every module should have a docstring at the top:

```python
"""Task execution utilities for asynchronous processing.

This module provides classes and functions for executing tasks
concurrently with support for timeouts, retries, and error handling.
"""

from typing import List
...
```

## Code Organization

### Import Order

Organize imports in the following order (with blank lines between groups):

1. Standard library imports
2. Third-party library imports
3. Local application imports

```python
import os
import sys
from typing import List, Dict

import numpy as np
import pandas as pd

from myproject.core import Engine
from myproject.utils import helpers
```

Use `isort` to automatically organize imports:

```bash
isort .
```

### File Structure

Organize files in the following order:

1. Module docstring
2. Imports
3. Constants
4. Type definitions
5. Classes
6. Functions

## Naming Conventions

- **Functions and variables**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private members**: `_leading_underscore`
- **Protected members**: `_single_leading_underscore`
- **Type variables**: `PascalCase` with `T` prefix: `TValue`, `TKey`

## Pre-commit Checks

Before committing code, ensure:

1. ✅ Code is formatted with Black: `black .`
2. ✅ Imports are sorted: `isort .`
3. ✅ Type checks pass: `mypy .`
4. ✅ All functions have type annotations
5. ✅ All functions have complete docstrings
6. ✅ Tests pass: `pytest`
