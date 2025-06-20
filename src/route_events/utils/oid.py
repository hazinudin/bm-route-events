from sqlalchemy import Engine, inspect, text
import os


def has_objectid(
        table: str,
        sql_engine: Engine
) -> bool:
    """
    Returns True if the table has Object ID column.
    """

    inspector = inspect(sql_engine)
    columns = inspector.get_columns(table)

    for col in columns:
        if col['name'] == 'objectid':
            return True

    return False


def generate_objectid(
        schema: str,
        table: str,
        sql_engine: Engine,
        oid_count: int
) -> list:
    """
    Generate list containing new Object ID.
    """
    with sql_engine.connect() as conn:
        # Check if PL/SQL function is available
        fn_exists = conn.execute(
            text("select count(*) from user_objects where object_name = 'GENEERATE_OID'")
        ).scalar()

        # If the function does not exists, then create the function
        if not fn_exists:
            with open(os.path.dirname(__file__) + '/generate_oid_function.sql') as sqlf:
                conn.execute(text(sqlf.read()))

        oids = [
            row[0] for row in conn.execute(text(f"select * from generate_oid({oid_count}, '{table}', '{schema}')"))
        ]
    
    return oids