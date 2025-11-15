Contributing
============

This section provides guidelines for contributing to the Nexus project.

Getting Started
-------------

We welcome contributions! Here's how you can help improve the Nexus library:

1. Fork the repository
2. Create a new branch for your changes
3. Make your changes
4. Submit a pull request

Development Setup
---------------

1. Clone the repository:

   .. code-block:: bash

       git clone https://github.com/nshaibu/nexus.git
       cd nexus

2. Create a virtual environment:

   .. code-block:: bash

       python -m venv venv
       source venv/bin/activate  # On Windows: venv\Scripts\activate

3. Install development dependencies:

   .. code-block:: bash

       pip install -r requirements-tests.txt
       pip install -r requirements-build.txt

Running Tests
-----------

Run the test suite:

.. code-block:: bash

    pytest

For coverage report:

.. code-block:: bash

    pytest --cov=nexus --cov-report=html

Code Style
---------

We use Black for code formatting:

.. code-block:: bash

    black nexus

Documentation
------------

To build the documentation:

1. Install Sphinx:

   .. code-block:: bash

       pip install sphinx sphinx-rtd-theme

2. Build the docs:

   .. code-block:: bash

       cd docs
       make html

The built documentation will be in `docs/build/html`.

Making Changes
------------

1. Create a new branch:

   .. code-block:: bash

       git checkout -b feature-name

2. Make your changes
3. Add tests for new functionality
4. Update documentation as needed
5. Run the test suite
6. Commit your changes:

   .. code-block:: bash

       git add .
       git commit -m "Description of changes"

7. Push to your fork:

   .. code-block:: bash

       git push origin feature-name

8. Create a Pull Request

Pull Request Guidelines
--------------------

- Include tests for new functionality
- Update documentation for changes
- Follow the existing code style
- Add an entry to CHANGELOG.md
- Keep commits focused and atomic

Reporting Issues
--------------

When reporting issues, please include:

- Detailed description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Version information:
  - Python version
  - Nexus version
  - OS version
- Any relevant logs or error messages

License
-------

By contributing, you agree that your contributions will be licensed under the GNU GPL-3.0 License.