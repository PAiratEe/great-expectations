from datahub.metadata.com.linkedin.pegasus2avro.dataset import FineGrainedLineage, FineGrainedLineageUpstreamType, \
    FineGrainedLineageDownstreamType, Upstream, UpstreamLineage, DatasetLineageType

import great_expectations as gx
import clickhouse_connect
import psycopg2.extras
from great_expectations.core.batch import RuntimeBatchRequest, BatchRequest
import pandas as pd
import argparse

from datahub.emitter.rest_emitter import DatahubRestEmitter
import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.metadata.schema_classes import (
    UpstreamLineageClass,
    UpstreamClass,
    FineGrainedLineageClass,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper

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
    host="postgresql.default.svc.cluster.local",
    database="postgres",
    user="postgres",
    password="s2KoXMe7jE",
    port=5432
)

pg_cursor = pg_client.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def datasetUrn(platform, tbl):
    return builder.make_dataset_urn(platform, tbl)


def fldUrn(platform, tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(platform, tbl), fld)


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

    batch_request_sql = RuntimeBatchRequest(
        datasource_name="postgres_ds",
        data_connector_name="pg_tables",
        data_asset_name=table,
        runtime_parameters={
            "query": f"""
                SELECT *
                FROM {table}
                WHERE updated_at::date = '{date}'
            """
        },
        batch_identifiers={"default_identifier_name": f"run_{date}"}
    )

    pg_cursor.execute(
        f"""
        SELECT *
        FROM {table}
        WHERE updated_at::date = %s
        """,
        (date,),
    )

    rows = pg_cursor.fetchall()
    df = pd.DataFrame(rows)

    batch_request_pandas = RuntimeBatchRequest(
        datasource_name="my_filesystem_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=f"{table}_runtime",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": f"run_{date}"},
        batch_spec_passthrough={"ge_batch_kwargs": {"result_format": "COMPLETE"}},
    )

    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request_sql,
                "expectation_suite_name": suite_name
            }
        ]
    )

    results_pandas = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request_pandas,
                "expectation_suite_name": suite_name
            }
        ]
    )

    run_results = results['run_results']
    # print(f"[GE metadata] {run_results}")

    run_results_pandas = results_pandas['run_results']
    # print(f"[GE data to CH] {run_results_pandas}")

    mask = [True] * len(df)

    for _, run_result in results_pandas["run_results"].items():
        validation_result = run_result.get("validation_result", {})
        batch_spec = validation_result.get("meta", {}).get("batch_spec", {})
        data_asset_name = batch_spec.get("data_asset_name", "")

        if not data_asset_name.endswith("_runtime"):
            continue

        for res in validation_result.get("results", []):
            success = res.get("success", True)
            col = res["expectation_config"]["kwargs"].get("column")

            if not success:
                failed_indices = res["result"].get("unexpected_index_list") or []
                failed_values = res["result"].get("partial_unexpected_list", [])

                if failed_indices:
                    for i in failed_indices:
                        if 0 <= i < len(mask):
                            mask[i] = False

                elif failed_values and col in df.columns:
                    for idx, row in df.iterrows():
                        if row.get(col) in failed_values:
                            mask[idx] = False

    good_df = df[[m for m in mask]]

    print(f"[GE] Raw rows: {len(df)}")
    print(f"[GE] Good rows: {len(good_df)}")
    print(f"[GE] Bad rows: {len(df) - len(good_df)}")

    good_df = good_df.replace({pd.NaT: None})
    good_df = good_df.where(pd.notnull(good_df), None)

    cols = list(good_df.columns)
    data_for_ch = good_df.values.tolist()

    ch_client.insert(
        table,
        data_for_ch,
        column_names=cols,
    )

    print("[GE] CLEAN DATA LOADED INTO CLICKHOUSE. Validation complete.")

    fineGrainedLineages = [
        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=[fldUrn("postgres", f"public.{table}", col)],
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[fldUrn("clickhouse", f"DatabaseNameToBeIngested.default.{table}", col)],
        )
        for col in cols
    ]

    upstream = Upstream(
        dataset=datasetUrn("postgres", f"public.{table}"),
        type=DatasetLineageType.TRANSFORMED,
    )

    fieldLineages = UpstreamLineage(
        upstreams=[upstream],
        fineGrainedLineages=fineGrainedLineages,
    )

    lineageMcp = MetadataChangeProposalWrapper(
        entityUrn=datasetUrn("clickhouse", f"DatabaseNameToBeIngested.default.{table}"),
        aspect=fieldLineages,
    )

    print("[CLICKHOUSE URN] ", datasetUrn("clickhouse", f"DatabaseNameToBeIngested.default.{table}"))

    print(lineageMcp)

    emitter = DatahubRestEmitter("http://datahub-datahub-gms:8080",
                                 "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjlkOTdhMzAwLTQyYmItNGMxMC04MWMzLTIzMjJlMTZhMmQzNyIsInN1YiI6ImRhdGFodWIiLCJpc3MiOiJkYXRhaHViLW1ldGFkYXRhLXNlcnZpY2UifQ.CkAxNq5Kyx4tsc2KCDFROUR8EUbMIgdlmpAHOizZcGg"
                                 )
    # urn: li:schemaField: (urn:li:dataset:(urn:li:dataPlatform:postgres, postgres.public.sample_data_users, PROD), address__country_code)
    # urn: li:schemaField: (urn:li:dataset:(urn:li:dataPlatform:clickhouse, DatabaseNameToBeIngested.default.sample_data_users, PROD), address__country_code)

    emitter.emit(lineageMcp)

    print("[DataHub] Column lineage Postgres → ClickHouse sent.")
    print("[GE] Validation complete.")


if __name__ == "__main__":
    main()
