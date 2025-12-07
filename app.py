import great_expectations as gx
import clickhouse_connect
from great_expectations.core.batch import RuntimeBatchRequest, BatchRequest
import pandas as pd
import argparse

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

# bad_conditions = []
#
# run_results = results.get("run_results", {})
# for _, run_result in run_results.items():
#     validation_result = run_result.get("validation_result", {})
#
#     for res in validation_result.get("results", []):
#         exp = res["expectation_config"]
#         result_block = res["result"]
#
#         if not res.get("success"):
#             column = exp["kwargs"].get("column")
#             unexpected_list = result_block.get("unexpected_list")
#             unexpected_values = result_block.get("partial_unexpected_list")
#
#             if unexpected_list:
#                 bad_conditions.append(f"{column} IN ({','.join(map(lambda x: "'" + str(x) + "'", unexpected_list))})")
#
# if bad_conditions:
#     clean_filter = " ".join([f"NOT ({c})" for c in bad_conditions])
# else:
#     clean_filter = "1 = 1"
#
# clean_table = f"validated.{table}"
#
# client.command(f"""
#     INSERT INTO {clean_table}
#     SELECT *
#     FROM {table}
#     WHERE toDate(updated_at) = '{date}' AND {clean_filter}
# """)
#
# print("GE COMPLETE. CLEAN DATA LOADED.")
