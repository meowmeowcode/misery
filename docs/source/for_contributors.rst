.. _for_contributors:

For contributors
================

Clone the repository:

.. code-block:: bash

    git clone https://github.com/meowmeowcode/misery.git
    cd misery

Install dependencies:

.. code-block:: bash

    poetry install

Check code with MyPy:

.. code-block:: bash

    poetry run mypy

Run tests:

.. code-block:: bash

    poetry run docker-compose up -d
    poetry run py.test tests/

Auto-format code:

.. code-block:: bash

    poetry run black .
