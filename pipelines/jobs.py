"""Small Airflow callables: heavy dependencies are imported only during execution."""
def build_silver():
    from .silver import build_silver as build
    return build()


def verify_silver(snapshot_id):
    from .silver import validate_snapshot
    return validate_snapshot(snapshot_id)


def build_gold(silver_snapshot_id):
    from .gold import build_gold as build
    return build(silver_snapshot_id=silver_snapshot_id)


def verify_gold(snapshot_id):
    from .gold import validate_snapshot
    return validate_snapshot(snapshot_id)
