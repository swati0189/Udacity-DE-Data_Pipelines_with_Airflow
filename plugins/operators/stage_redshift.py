from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    DAG operator to populate staging tables from source files.
    
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 s3_bucket,
                 s3_path,
                 aws_key,
                 aws_secret,
                 region,
                 copy_json_option,
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.copy_json_option = copy_json_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator begin execute')

        # connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connected with {self.redshift_conn_id}")

        sql_stmt = f"""
            COPY {self.table_name} 
                FROM 's3://{self.s3_bucket}/{self.s3_path}' 
                ACCESS_KEY_ID '{self.aws_key}'
                SECRET_ACCESS_KEY '{self.aws_secret}'
                REGION '{self.region}'
                JSON '{self.copy_json_option}'
                TIMEFORMAT as 'epochmillisecs'
                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        """
        self.log.info(f"copy sql: {sql_stmt}")

        redshift_hook.run(sql_stmt)
        self.log.info(
            f"StageToRedshiftOperator copy complete - {self.table_name}")