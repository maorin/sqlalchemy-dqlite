rqlite dialect for SQLAlchemy
==============================

This is the dqlite dialect driver for SQLAlchemy.
It was adapted from the driver for rqlite


installation
------------

To install this dialect run::

    $ pip install sqlalchemy_dqlite

or from source::

    $ pip install -r ./requirements.txt
    $ python ./setup.py install


usage
-----

To start using this dialect::

    from sqlalchemy import create_engine
    engine = create_engine('dqlite+pydqlite://localhost:4001/', echo=True)

If you don't want to install this library (for example during development) add
this folder to your PYTHONPATH and register this dialect with SQLAlchemy::

    from sqlalchemy.dialects import registry
    registry.register("dqlite.pydqlite", "sqlalchemy_dqlite.pydqlite", "dialect")

testing
-------

you need to have pytest and pytest-cov installed::

    $ pip install pytest pytest-cov

Run the test suite::

    $ python setup.py test



more info
---------

 * http://www.sqlalchemy.org/
 * https://github.com/canonical/dqlite
 * https://github.com/rqlite/rqlite
