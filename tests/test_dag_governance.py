from pathlib import Path
from airflow.models import DagBag


DAG_FOLDER = Path(__file__).resolve().parents[1] / "dags"


def test_dags_import_without_errors():
    dag_bag = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)
    assert len(dag_bag.import_errors) == 0, dag_bag.import_errors


def test_retail_dags_have_required_governance_fields():
    dag_bag = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)

    retail_dags = {
        dag_id: dag
        for dag_id, dag in dag_bag.dags.items()
        if dag_id.startswith("retail_")
    }

    assert retail_dags, "No retail DAGs found"

    for dag_id, dag in retail_dags.items():
        assert dag.description, f"{dag_id} is missing description"
        assert dag.tags, f"{dag_id} is missing tags"
        assert dag.owner, f"{dag_id} is missing owner"
        assert dag.max_active_runs is not None, f"{dag_id} is missing max_active_runs"
        assert dag.doc_md, f"{dag_id} is missing doc_md"
        assert dag.default_args.get("retries", 0) >= 2, f"{dag_id} must have retries >= 2"
