from etl_framework.dependencies.logger_utils import get_logger
from etl_framework.dependencies.spark import create_spark_session
from etl_framework.jobs.demo_job import DemoJob

jobs = {
    'demo_job': DemoJob
}


def run(parameters):
    logger = get_logger()

    for parameter, value in parameters.items():
        logger.info(
            'Param {param}: {value}'.format(param=parameter, value=value))

    spark_config = parameters['spark_config']
    spark = create_spark_session(spark_config=spark_config)

    job_names = parameters['job_names']

    for job_name in job_names:
        if job_name in jobs:
            jobs[job_name](spark, job_name).run()
        else:
            raise NotImplementedError(
                f"ETL code for {job_name} is not implemented")
