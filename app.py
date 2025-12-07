import great_expectations as gx
import clickhouse_connect
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

client = clickhouse_connect.get_client(
    host="clickhouse.default.svc.cluster.local",
    username="default",
    password="dCkUgJH3JI",
    port=8123
)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--table", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day", required=True)

    args = parser.parse_args()
    table = args.table
    clean_table = f"analytics.{table}"
    date = f"{args.year}-{args.month}-{args.day}"
    suite_name = f"{table}_suite"
    checkpoint_name = DEFAULT_CHECKPOINT

    batch_request = BatchRequest(
        datasource_name="clickhouse_ds",
        data_connector_name="ch_tables",
        data_asset_name=f"default.{table}",
        batch_spec_passthrough={
            "query": f"""
                SELECT *
                FROM {table}
                WHERE toDate(updated_at) = '{date}'
            """
        }
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

    if not bad_indices:
        print("[GE] All rows passed validation. Loading all rows...")

        client.command(f"""
            INSERT INTO {clean_table}
            SELECT *
            FROM {table}
            WHERE toDate(updated_at) = '{date}'
        """)

        print("[GE] COMPLETE — all rows valid and loaded.")
        return

    idx_list_sql = ",".join(str(i) for i in bad_indices)

    bad_ids_rows = client.query(f"""
        SELECT id
        FROM (
            SELECT id, row_number() OVER () - 1 AS idx
            FROM {table}
            WHERE toDate(updated_at) = '{date}'
        )
        WHERE idx IN ({idx_list_sql})
    """).result_rows

    bad_ids = [float(row[0]) for row in bad_ids_rows]
    print(f"[GE] bad id values: {bad_ids}")

    bad_ids_filter = ch_float_list(bad_ids)

    clean_filter = f"id NOT IN ({bad_ids_filter})"

    print(f"""
        INSERT INTO {clean_table}
        SELECT *
        FROM {table}
        WHERE toDate(updated_at) = '{date}'
          AND {clean_filter}
    """)

    client.command(f"""
        INSERT INTO {clean_table}
        SELECT *
        FROM {table}
        WHERE toDate(updated_at) = '{date}'
          AND {clean_filter}
    """)

    print("[GE] CLEAN DATA LOADED. Validation complete.")


if __name__ == "__main__":
    main()