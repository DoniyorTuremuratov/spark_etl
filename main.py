from etl_framework.run import run

if __name__ == '__main__':
    parameters = {
        'spark_config': {
            'spark.master': 'local[*]',
        },
        'job_names': ['demo_job']
    }
    run(parameters)
