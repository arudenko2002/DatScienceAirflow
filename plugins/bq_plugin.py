import logging
import time
from datetime import datetime, timedelta
import uuid
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator
from bq_hook import BigQueryHook


class BqWriteToTableOperator(BaseOperator):
    """

    Creates a daily partition in BigQuery table,
    based on provided execution time and SQL.
    With option to create a shard instead of partition

    """
    ui_color = '#33F3FF'
    template_fields = ('sql',
                       'destination_table',
                       'partition')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_table,
                 partition = None,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 shard = False,
                 write_disposition = 'WRITE_TRUNCATE',
                 *args, **kwargs):
        self.sql = sql
        self.destination_table = destination_table
        self.partition = partition
        self.bigquery_conn_id=bigquery_conn_id
        self.use_legacy_sql=use_legacy_sql
        self.shard = shard
        self.write_disposition = write_disposition
        super(BqWriteToTableOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info('Writing data to %s from SQL: %s',
                     self.destination_table,
                     self.sql)

        #prepare parameters for passing to the BQ hook for execution

        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        table_name = dst_table_array[len(dst_table_array) - 1]
        dataset_name = dst_table_array[len(dst_table_array) - 2]
        #logging.info('partition: %s', partition)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        hook.write_to_table(sql = self.sql,
                            destination_dataset = dataset_name,
                            destination_table = '{}{}{}'.format(table_name,
                                                                '_' if self.shard else '$',
                                                                self.partition.replace('-', '')) if self.partition else table_name,
                            write_disposition=self.write_disposition)

class GcsToBqOperator(BaseOperator):

    ui_color = '#88F8FF'
    template_fields = ('gcs_uris',
        'destination_table')

    @apply_defaults
    def __init__(
        self,
        gcs_uris,
        destination_table,
        schema,
        source_format = 'CSV',
        field_delimiter = ',',
        bigquery_conn_id = 'bigquery_default',
        write_disposition = 'WRITE_TRUNCATE',
        *args, **kwargs):

        self.gcs_uris = gcs_uris
        self.destination_table = destination_table
        self.schema = schema
        self.source_format = source_format
        self.field_delimiter = field_delimiter
        self.bigquery_conn_id = bigquery_conn_id
        self.write_disposition = write_disposition
        super(GcsToBqOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
        logging.info('Importing data from %s to %s',
                     self.gcs_uris,
                     self.destination_table)

        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        table_name = dst_table_array[len(dst_table_array) - 1]
        dataset_name = dst_table_array[len(dst_table_array) - 2]

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        hook.import_from_gcs( 
                        gcs_uris = self.gcs_uris,
                        dataset = dataset_name,
                        table = table_name,
                        schema = self.schema,
                        source_format = self.source_format,
                        field_delimiter = self.field_delimiter,
                        write_disposition = self.write_disposition)
    

    
class BqIncrementalLoadDataOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary

    """
    ui_color = '#33FFEC'
    template_fields = ('sql',
                       'partition_list_sql',
                       'source_table',
                       'destination_table',
                       'last_update_value',
                       'execution_time'
    )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 partition_list_sql,
                 source_table,
                 destination_table,
                 source_partition_column,
                 destination_partition_column,
                 last_update_column, 
                 last_update_value,
                 execution_time,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 *args, 
                 **kwargs):
        self.sql = sql
        self.partition_list_sql = partition_list_sql,
        self.source_table = source_table
        self.destination_table = destination_table
        self.source_partition_column = source_partition_column
        self.destination_partition_column = destination_partition_column
        self.last_update_column = last_update_column
        self.last_update_value = last_update_value
        self.execution_time = execution_time
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        super(BqIncrementalLoadDataOperator, self).__init__(*args, **kwargs)
    

    def execute(self, context):
        
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        dst_table = dst_table_array[len(dst_table_array) - 1]
        dst_dataset = dst_table_array[len(dst_table_array) - 2]

        # check if we need to back-off failed load 
        # we do this if we find any value with higher last update value 
        # than the one passed as a parameter

        
        backoff_partitions_sql = """
            select distinct {} 
            from `{}` 
            where {} > timestamp('{}')
            and load_datetime <= timestamp_add(timestamp('{}'), interval 24 hour)
            """.format(
                self.destination_partition_column,
                self.destination_table,
                self.last_update_column,
                self.last_update_value,
                self.execution_time)

        logging.info('Checking for partitions to back off')
        logging.info('Executing SQL: ' + backoff_partitions_sql)
        job_id = hook.execute_query(backoff_partitions_sql, use_legacy_sql=False)
        backoff_partitions =  hook.fetch(job_id)

        if len(backoff_partitions) > 0:
            logging.info('Backing off previously loaded partitions')
            for partition in backoff_partitions:
                #move partition into a temp table so we could do DML on it
                temp_table_name = 'temp_{}_{}'.format(
                        dst_table,
                        partition[self.destination_partition_column].replace('-', '')
                )

                create_temp_table_sql = """
                select *
                from `{}`
                where _partitiontime = timestamp('{}')
                """.format(
                    self.destination_table,
                    partition[self.destination_partition_column]
                )

                logging.info('Copying partition to an unpartitioned temp table')
                logging.info('Executing SQL: ' + create_temp_table_sql)

                hook.write_to_table(sql = create_temp_table_sql,
                    destination_dataset = dst_dataset,
                    destination_table = temp_table_name,
                    write_disposition='WRITE_TRUNCATE'
                )

                # delete all the records with timestamp later than last loaded
                delete_records_sql = """
                delete from `{}.{}` 
                where load_datetime > timestamp('{}')
                and load_datetime <= timestamp_add(timestamp('{}'), interval 24 hour)
                """.format(
                    dst_dataset,
                    temp_table_name,
                    self.last_update_value,
                    self.execution_time
                )

                logging.info('Deleting all the records with timestamp later than last loaded')
                logging.info('Executing SQL: ' + delete_records_sql)

                job_id = hook.execute_query(delete_records_sql, use_legacy_sql=False)
                result =  hook.fetch(job_id)

                #reinsert updated partition into main table
                replace_partition_sql = """
                select * from `{}.{}` 
                """.format(
                    dst_dataset,
                    temp_table_name
                )
                logging.info('Reinsert updated partition into main table')
                logging.info('Executing SQL: ' + replace_partition_sql)

                hook.write_to_table(sql = replace_partition_sql,
                    destination_dataset = dst_dataset,
                    destination_table = '{}${}'.format(
                                dst_table,
                                partition[self.destination_partition_column].replace('-', '')),
                    write_disposition='WRITE_TRUNCATE'
                )

                #delete temp table
                logging.info('Deleting temp table')
                hook.delete_table(dst_dataset, temp_table_name)

        else:
            logging.info('No need to back off')
            
        #get list of partitions to load
        logging.info('Getting list of partitions to load')
        job_id = hook.execute_query(self.partition_list_sql, use_legacy_sql=False)
        partition_list =  hook.fetch(job_id)

        # load partitions
        logging.info('Loading partitions')
        partition_array = []
        for partition in partition_list:
            load_partition_sql = self.sql.replace(
                '#{}#'.format(self.destination_partition_column), 
                partition[self.destination_partition_column])
            logging.info('Executing SQL: ' + load_partition_sql)
            hook.write_to_table(sql = load_partition_sql,
                        destination_dataset = dst_dataset,
                        destination_table = '{}${}'.format(
                            dst_table,
                            partition[self.destination_partition_column].replace('-', '')),
                        write_disposition='WRITE_APPEND')

            partition_array.append(partition[self.destination_partition_column])

        return partition_array    

class BqPlugin(AirflowPlugin):
    name = "BigQuery Plugin"
    operators = [BqWriteToTableOperator,
                 GcsToBqOperator,
                 BqIncrementalLoadDataOperator,
                 ]
