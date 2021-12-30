# ELT Pipeline using Airflow
In this project, I have created an ELT pipeline for a `Data Warehouse` using Airflow DAG and operators to do following tasks in a pipeline:
1. Staging the data from `s3 bucket` to `AWS Redshift`.(Implemented StageToRedshiftOperator found at `plugins/operators/stage_redshift.py`).
2.  Creating the dimension tables in the Redshift.(Implemented LoadDimensionOperator found at `plugins/operators/load_dimension.py`).
3.  Creating the fact tables in the Redshift.(Implemented LoadFactOperator found at `plugins/operators/load_fact.py`).
4. Running Quality checks on Fact and Dimensional tables. (Implemented DataQualityOperator found at `plugins/operators/data-quality.py`).

**Note:** The Airflow DAG Script can be found at `dags/elt_dag.py`.
The custom SQL Queries for `AWS REDSHIFT` can be found at `plugins/helpers/sql_queries.py`.
## Project Requirements
- Apache Airflow
- Python 3.x
- AWS Credentials with S3 and RedShift Services Enabled

## Running the Project
1. Clone the repository.
2. Install the Project Requirements.
3. Configure the AWS Credentials by setting `AWS_SECRET` and `AWS_KEY` properties in the environment.
4. Run Apache Airflow locally using instructions provided in the [link](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html#)
5. Copy the files in `plugins/*` folder of the local repository to airflow plugins directory i.e `~/airflow/plugins/`.
6. Copy the `dags/etl_dag.py` file to the airflow dags directory i.e `~/airflow/dags/`. The airflow scheduler will automatically invoke the dag execution which can be seen from the Airflow UI.

## Author
- [@maheshjindal](https://www.github.com/maheshjindal)