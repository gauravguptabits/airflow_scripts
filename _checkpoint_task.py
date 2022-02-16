from airflow.providers.postgres.hooks.postgres import PostgresHook

def data_ingestion_success_ckpt_hook():
    return

def data_ingestion_failure_ckpt_hook():
    return

def data_ingestion_read_ckpt_hook():
    # Implement Airflow hook.
    query = ''
    pg_hook = PostgresHook(conn_name_attr='', schema='')
    ckpt_record = pg_hook.get_first(query)

    # TODO: Pass information to next task.
    return
