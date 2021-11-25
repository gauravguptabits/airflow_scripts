from airflow.providers.postgres.hooks.postgres import PostgresHook

def data_ingestion_success_ckpt_hook():
    return

def data_ingestion_failure_ckpt_hook():
    return

def data_ingestion_read_ckpt_hook():
    # Implement Airflow hook.
    schema = 'migration_checkpoints'
    database = 'Migration_Catalogue'
    table = 'Checkpoint'
    
    query = f'select * from {schema}."{table}";'
    pg_hook = PostgresHook(conn_name_attr='postgres_local',
                           default_conn_name='postgres_local',
                           schema=database)
    pg_hook.test_connection()
    ckpt_record = pg_hook.get_pandas_df(query)
    print(ckpt_record.head())
    # TODO: Pass information to next task.
    return ckpt_record.to_dict()
