FROM ednarb29/docker-airflow:2.2.3

ADD 'requirements.txt' .
RUN pip install -r requirements.txt

