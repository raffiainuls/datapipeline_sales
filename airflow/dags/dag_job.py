from datetime import datetime, timedelta
import json, time, requests
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.exceptions import AirflowException

# --- KONFIG ---
SPARK_REST_BASE = "http://spark-master:6066"  # hostname service Spark master kalian
APP_PY = "file:/opt/spark/spark-apps/fact_sales/main.py"  # path yang terlihat oleh master & worker
CLIENT_SPARK_VERSION = "3.5.1"
SPARK_MASTER = "spark://spark-master:7077"  # cluster master, bukan REST port

def _submit():
    ctx = get_current_context()
    # Payload sesuai schema CreateSubmissionRequest
    payload = {
        "action": "CreateSubmissionRequest",
        # App python biasanya boleh diisi juga di appResource (tak wajib untuk SparkSubmit, tapi aman)
        "appResource": APP_PY,
        "clientSparkVersion": CLIENT_SPARK_VERSION,
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        # Argumen ke spark-submit: pertama adalah path script .py, sisanya argumen script (jika ada)
        "appArgs": [APP_PY],
        "environmentVariables": {
            "SPARK_ENV_LOADED": "1"
            # Kalau butuh, bisa tambahkan MASTER di sini, tapi spark.master di sparkProperties sudah cukup
            # "MASTER": SPARK_MASTER
        },
        "sparkProperties": {
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "cluster",
            "spark.app.name": "fact_sales_job",
            "spark.driver.supervise": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "file:/tmp/spark-events",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "1",
            # === OPSIONAL: kalau sink ke ClickHouse via JDBC/connector, aktifkan dan isi JAR yang benar ===
            # "spark.jars": "file:/opt/spark/jars/clickhouse-jdbc-0.6.0.jar,file:/opt/spark/jars/httpclient-4.5.13.jar,file:/opt/spark/jars/httpcore-4.4.13.jar"
            # atau kalau pakai connector lain, sesuaikan.
        }
    }

    r = requests.post(f"{SPARK_REST_BASE}/v1/submissions/create",
                      headers={"Content-Type": "application/json"},
                      data=json.dumps(payload),
                      timeout=60)
    # 400 -> schema salah; 200 tapi "success": false -> ditolak master
    try:
        r.raise_for_status()
    except Exception:
        raise AirflowException(f"REST submit HTTP {r.status_code}: {r.text}")

    resp = r.json()
    if not resp.get("success", False):
        raise AirflowException(f"REST submit rejected: {resp}")

    submission_id = resp.get("submissionId")
    if not submission_id:
        raise AirflowException(f"Tidak ada submissionId di response: {resp}")
    # simpan ke XCom
    ctx["ti"].xcom_push(key="submission_id", value=submission_id)

def _wait_until_finish(timeout_minutes: int = 60, poll_seconds: int = 5):
    ctx = get_current_context()
    ti = ctx["ti"]
    submission_id = ti.xcom_pull(key="submission_id", task_ids="submit_fact_sales_via_restapi")
    if not submission_id:
        raise AirflowException("submission_id kosong")

    deadline = time.time() + timeout_minutes * 60
    last_state = None
    while True:
        r = requests.get(f"{SPARK_REST_BASE}/v1/submissions/status/{submission_id}", timeout=30)
        r.raise_for_status()
        resp = r.json()
        state = resp.get("driverState")
        if state != last_state:
            print(f"[Spark REST] driverState={state}")
            last_state = state

        # Sukses
        if state == "FINISHED":
            return

        # Gagal / di-kill / unknown
        if state in ("ERROR", "FAILED", "KILLED", "UNKNOWN"):
            raise AirflowException(f"Job gagal: state={state}, detail={resp}")

        # Masih jalan
        if time.time() > deadline:
            # Opsional: coba kill
            try:
                requests.post(f"{SPARK_REST_BASE}/v1/submissions/kill/{submission_id}", timeout=15)
            except Exception:
                pass
            raise AirflowException(f"Timeout menunggu job selesai. Last state={state}")

        time.sleep(poll_seconds)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fact_sales_etl_restapi',
    default_args=default_args,
    start_date=datetime(2025, 8, 15),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=["spark", "rest", "clickhouse"]
) as dag:

    submit = PythonOperator(
        task_id='submit_fact_sales_via_restapi',
        python_callable=_submit
    )

    wait_finish = PythonOperator(
        task_id='wait_until_finished',
        python_callable=_wait_until_finish,
        op_kwargs={"timeout_minutes": 60, "poll_seconds": 5}
    )

    submit >> wait_finish
