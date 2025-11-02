from flask import Flask, request, jsonify
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd

app = Flask(__name__)

GE_DIR = "/ge/great_expectations"
DEFAULT_CHECKPOINT = "spark_streaming_checkpoint"
DEFAULT_SUITE = "spark_streaming_suite"

context = gx.DataContext(GE_DIR)

@app.route('/validate', methods=['POST'])
def validate():
    try:
        req = request.get_json()
        rows = req.get("data", [])
        suite_name = req.get("suite_name", DEFAULT_SUITE)
        checkpoint_name = req.get("checkpoint_name", suite_name.replace("_suite", "_checkpoint"))

        if not rows:
            return jsonify({"error": "No data provided", "result": []}), 400
        df = pd.DataFrame(rows)
        print("DEBUG DATA:", df.to_dict(orient="records"))

        batch_request = RuntimeBatchRequest(
            datasource_name="my_filesystem_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="spark_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default_identifier"},
            batch_spec_passthrough={"ge_batch_kwargs": {"result_format": "COMPLETE"}}
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
        mask = [True] * len(df)
        for run_id, run_result in run_results.items():
            validation_result = run_result.get("validation_result", {})
            for res in validation_result.get("results", []):
                success = res.get("success", True)
                if not success:
                    failed_indices = res["result"].get("unexpected_index_list")
                    failed_values = res["result"].get("partial_unexpected_list", [])
                    print(f"⚠️ Failed expectation: {res['expectation_config']['expectation_type']}")
                    if failed_indices:
                        for i in failed_indices:
                            mask[i] = False
                    elif failed_values:
                        col = res["expectation_config"]["kwargs"].get("column")
                        mask = [not (row.get(col) in failed_values) for _, row in df.iterrows()]

                print(f"✅ Validation finished: {sum(mask)} passed, {len(mask) - sum(mask)} failed")
                return jsonify({"result": mask})

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e), "result": []}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
