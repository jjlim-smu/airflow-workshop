# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

import os
from etl_code.etl import transform, load

# NOTE: Generate Presigned URL
url = '# PASTE PRESIGNED URL HERE #'


# ---------- DEFINE YOUR DAG HERE ---------- #
