
from sqlalchemy import (
    create_engine,
)

from sqlalchemy_dqlite import pydqlite

from sqlalchemy.dialects import registry

def test_register():
    registry.register("dqlite.pydqlite", "sqlalchemy_dqlite.pydqlite", "dialect")
    #engine = create_engine('dqlite+pydqlite://localhost:4001/?detect_types=0&connect_timeout=3.0')
    #engine.dispose()
