######################
# Imports
######################
import html
import json
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vision import CloudVisionImageAnnotateOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.openai.operators.openai import OpenAIHook
from airflow.utils.trigger_rule import TriggerRule

######################
# Variables
######################


dag_id = "marketing_dag"
start_date = datetime(2025, 5, 26)
schedule = "@daily"

dag_args = {
    'job_name': dag_id,
    'owner': 'airflow',
}

BUCKET_NAME = 'marketing_materials_lh'
PREFIX = 'incoming/'
PROCESSED_PREFIX = 'processed/'


#####################
# Functions
#####################

# Remove folder from a list, leave only files
def filter_only_files(ti):
    files = ti.xcom_pull(task_ids="list_gcp_objects", key=None)
    filtered = [f for f in files if files and not f.endswith('/')]
    Variable.set("filtered_files", json.dumps(filtered))


# Remove unnecessary info from OCR result
def extract_text_from_annotation(ti, index):
    response = ti.xcom_pull(task_ids=f"brand_pipeline.annotate_image_{index}", key=None)
    if not response:
        print('No response received from annotate_image')
        return ""
    descriptions = response["textAnnotations"][0]["description"]
    return descriptions


# Extract brand name and additional info about brand using Chat GPT
def extract_brand_name_with_gpt(ti, index):
    ocr_text = ti.xcom_pull(task_ids=f"brand_pipeline.extract_text_{index}")
    if not ocr_text:
        return "No OCR text found."

    # Prompt
    messages = [
        {
            "role": "system",
            "content": "You are an assistant that extracts brand names from advertising text."
        },
        {
            "role": "user",
            "content": (
                "Here is the OCR text extracted from an image:\n\n"
                f"{ocr_text}\n\n"
                "Please identify and return the most likely brand name mentioned in the text.\n"
                "If no clear brand is found, reply with 'No brand detected.'\n"
                "Provide a short brand description.\n"
                "Respond only with valid JSON, no explanation.\n"
                "Respond in the following JSON format:\n"
                "{ \"brand_name\": \"brand_name\", \"description\": \"description\" }"
            )
        }
    ]

    # Call OpenAI API via the hook
    hook = OpenAIHook(conn_id="openai_default")
    response = hook.create_chat_completion(
        messages=messages,
        model="gpt-3.5-turbo"
    )

    result = json.loads(response[0].message.content.strip())

    ti.xcom_push(key="brand_name", value=html.escape(result['brand_name']))
    ti.xcom_push(key="description", value=html.escape(result['description']))


######################
# Operators
######################
with DAG(dag_id=dag_id,
         schedule_interval=schedule,
         start_date=start_date,
         catchup=False,
         default_args=dag_args) as dag:
    # Create input table
    table_create = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        conn_id="postgres_storage",
        sql="""
        CREATE TABLE IF NOT EXISTS materials
        (
        brand_name TEXT UNIQUE NOT NULL,
        "description" TEXT,
        marketing_materials TEXT[]
        );"""
    )

    # List files in a bucket
    load_materials = GCSListObjectsOperator(
        task_id="list_gcp_objects",
        gcp_conn_id="google_cloud_default",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        do_xcom_push=True
    )

    # Remove dir from files list
    filter_files = PythonOperator(
        task_id='filter_files_list',
        python_callable=filter_only_files
    )

    # Process files in parallel
    with TaskGroup(group_id=f"brand_pipeline") as tg:
        for index, file_name in enumerate(Variable.get('filtered_files', deserialize_json=True)):
            # Recognize text on image using Google Vision AI
            annotate_image = CloudVisionImageAnnotateOperator(
                task_id=f"annotate_image_{index}",
                request={
                    "image": {
                        "source": {
                            "image_uri": f"gs://{BUCKET_NAME}/{file_name}"
                        }
                    },
                    "features": [
                        {"type": "TEXT_DETECTION"}
                    ]
                },
                timeout=5,
                do_xcom_push=True
            )

            # Filter Vision AI output
            extract_text = PythonOperator(
                task_id=f"extract_text_{index}",
                python_callable=extract_text_from_annotation,
                op_kwargs={"index": index},
                do_xcom_push=True
            )

            # Sent processed text to GPT
            process_with_gpt = PythonOperator(
                task_id=f"process_with_gpt_{index}",
                python_callable=extract_brand_name_with_gpt,
                op_kwargs={"index": index},
            )

            # Write result to database, provide deduplication
            insert_into_db = SQLExecuteQueryOperator(
                task_id=f"insert_into_postgres_{index}",
                conn_id="postgres_storage",
                sql="""
                -- lock the row for this brand_name if it exists
                SELECT 1 FROM materials 
                WHERE brand_name = '{{ ti.xcom_pull(task_ids='brand_pipeline.process_with_gpt_""" + str(index) + """', key="brand_name") }}'
                FOR UPDATE;

                -- do the upsert with deduplicated links
                INSERT INTO materials (brand_name, description, marketing_materials)
                VALUES (
                    '{{ ti.xcom_pull(task_ids='brand_pipeline.process_with_gpt_""" + str(index) + """', key="brand_name") }}',
                    '{{ ti.xcom_pull(task_ids='brand_pipeline.process_with_gpt_""" + str(index) + """', key="description") }}',
                    ARRAY['""" + file_name.replace(PREFIX, '') + """']
                )
                ON CONFLICT (brand_name) DO UPDATE
                SET
                    description = EXCLUDED.description,
                    marketing_materials = (
                        SELECT ARRAY(
                            SELECT DISTINCT unnest(materials.marketing_materials || EXCLUDED.marketing_materials)
                        )
                    );
                """
            )

            # Move processed files to folder processed/
            move_file_to_processed = GCSToGCSOperator(
                task_id=f"move_file_in_gcs_{index}",
                source_bucket=BUCKET_NAME,
                source_object=file_name,
                destination_bucket=BUCKET_NAME,
                destination_object=file_name.replace(PREFIX, PROCESSED_PREFIX),
                move_object=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            annotate_image >> extract_text >> process_with_gpt >> insert_into_db >> move_file_to_processed

######################
# DAGs flow
######################
table_create >> load_materials >> filter_files >> tg
