import great_expectations as gx
import clickhouse_connect
import psycopg2.extras
from great_expectations.core.batch import RuntimeBatchRequest, BatchRequest
import pandas as pd
import argparse


def ch_float_list(values):
    """
    Формирует корректный список float литералов для ClickHouse:
    id IN (1.0, 3.5, 9.0)
    """
    return ",".join(str(float(v)) for v in values)


GE_DIR = "/ge/great_expectations"
DEFAULT_CHECKPOINT = "spark_streaming_checkpoint"
DEFAULT_SUITE = "spark_streaming_suite"

context = gx.DataContext(GE_DIR)

ch_client = clickhouse_connect.get_client(
    host="clickhouse.default.svc.cluster.local",
    username="default",
    password="dCkUgJH3JI",
    port=8123
)

pg_client = psycopg2.connect(
    host="postgres.default.svc.cluster.local",
    database="public",
    user="postgres",
    password="s2KoXMe7jE",
    port=5432
)

pg_cursor = pg_client.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--table", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day", required=True)

    args = parser.parse_args()
    table = args.table
    date = f"{args.year}-{args.month}-{args.day}"
    suite_name = f"{table}_suite"
    checkpoint_name = DEFAULT_CHECKPOINT

    batch_request = BatchRequest(
        datasource_name="postgres_ds",
        data_connector_name="pg_tables",
        data_asset_name=table,
        batch_spec_passthrough={
            "query": f"""
                SELECT *
                FROM {table}
                WHERE updated_at::date = '{date}'
            """
        },
        batch_identifiers = {"runtime_param": "batch1"}
    )

    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name
            }
        ]
    )

    run_results = results['run_results']
    print(f"[GE] {run_results}")

    bad_indices = set()

    run_results = results.get("run_results", {})
    for _, run_result in run_results.items():
        validation_result = run_result.get("validation_result", {})

        for res in validation_result.get("results", []):
            result_block = res.get("result", {})
            idxs = result_block.get("unexpected_index_list", [])
            if idxs:
                bad_indices.update(idxs)

    bad_indices = sorted(bad_indices)
    print(f"[GE] bad row indices: {bad_indices}")

    if bad_indices:
        idx_list_sql = ",".join(str(i) for i in bad_indices)

        pg_cursor.execute(
            f"""
            SELECT id
            FROM (
                SELECT id, row_number() OVER () - 1 AS idx
                FROM {table}
                WHERE updated_at::date = %s
            ) t
            WHERE idx IN ({idx_list_sql})
            """,
            (date,),
        )
        rows = pg_cursor.fetchall()
        bad_ids = [r["id"] for r in rows]
    else:
        bad_ids = []

    print(f"[GE] bad ids: {bad_ids}")

    pg_cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    cols = [r["column_name"] for r in pg_cursor.fetchall()]
    cols_csv = ", ".join(cols)

    if bad_ids:
        bad_ids_sql = ",".join(str(b) for b in bad_ids)
        where_clause = f"updated_at::date = %s AND id NOT IN ({bad_ids_sql})"
    else:
        where_clause = "updated_at::date = %s"

    pg_cursor.execute(
        f"""
            SELECT {cols_csv}
            FROM {table}
            WHERE {where_clause}
            """,
        (date,),
    )
    good_rows = pg_cursor.fetchall()

    print(f"[GE] good rows count: {len(good_rows)}")

    if not good_rows:
        print("[GE] No rows to load into ClickHouse. Finish.")
        pg_cursor.close()
        pg_client.close()
        return

    data_for_ch = [[row[col] for col in cols] for row in good_rows]

    print(f"[GE] inserting {len(data_for_ch)} clean rows into ClickHouse table {table}...")

    ch_client.insert(
        table,
        data_for_ch,
        column_names=cols,
    )

    print("[GE] CLEAN DATA LOADED INTO CLICKHOUSE. Validation complete.")

    pg_cursor.close()
    pg_client.close()


if __name__ == "__main__":
    main()
