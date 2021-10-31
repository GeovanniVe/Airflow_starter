import warnings
from typing import List, Optional, Union

from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import io
import os.path

AVAILABLE_METHODS = ['APPEND', 'REPLACE', 'UPSERT']


class S3ToPostgresOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift

    .. seealso::
        For more information on how to use this operator, take a look at the
        guide: :ref:`howto/operator:S3ToRedshiftOperator`

    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key
    :type s3_key: str
    :param postgres_conn_id: reference to a specific redshift database
    :type postgres_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
        If the AWS connection contains 'aws_iam_role' in ``extras``
        the operator will use AWS STS credentials with a token
        https://docs.aws.amazon.com/redshift/latest/dg/
        copy-parameters-authorization.html#copy-credentials
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param column_list: list of column names to load
    :type column_list: List[str]
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    :param method: Action to be performed on execution.
    Available ``APPEND``, ``UPSERT`` and ``REPLACE``.
    :type method: str
    :param upsert_keys: List of fields to use as key on upsert action
    :type upsert_keys: List[str]
    """
    template_fields = ('s3_bucket', 's3_key', 'schema', 'table', 'column_list',
                       'copy_options')
    template_ext = ()
    ui_color = '#99e699'

    def __init__(
        self,
        *,
        schema: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        postgres_conn_id: str = 'postgres_default',
        aws_conn_id: str = 'aws_default',
        verify=None,
        wildcard_match = False,
        column_list: Optional[List[str]] = None,
        copy_options: Optional[List] = None,
        autocommit: bool = False,
        method: str = 'APPEND',
        upsert_keys: Optional[List[str]] = None,
        **kwargs,
    ) -> None:

        if 'truncate_table' in kwargs:
            warnings.warn(
                """`truncate_table` is deprecated. 
                Please use `REPLACE` method.""",
                DeprecationWarning,
                stacklevel=2,
            )
            if kwargs['truncate_table']:
                method = 'REPLACE'
            kwargs.pop('truncate_table', None)

        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.column_list = column_list
        self.copy_options = copy_options or []
        self.autocommit = autocommit
        self.method = method
        self.upsert_keys = upsert_keys

        if self.method not in AVAILABLE_METHODS:
            raise AirflowException(f'Method not found! Available methods: '
                                   f'{AVAILABLE_METHODS}')

    def _build_copy_query(self, copy_destination: str,
                          credentials_block: str, copy_options: str) -> str:
        column_names = "(" + ", ".join(self.column_list) + ")" \
                       if self.column_list else ''
        return f"""
                    COPY {copy_destination} {column_names}
                    FROM 's3://{self.s3_bucket}/{self.s3_key}'
                    credentials
                    '{credentials_block}'
                    {copy_options};
        """

    def _get_table_primary_key(self, postgres_hook):
        sql = """
            select kcu.column_name
            from information_schema.table_constraints tco
                    join information_schema.key_column_usage kcu
                        on kcu.constraint_name = tco.constraint_name
                            and kcu.constraint_schema = tco.constraint_schema
                            and kcu.constraint_name = tco.constraint_name
            where tco.constraint_type = 'PRIMARY KEY'
            and kcu.table_schema = %s
            and kcu.table_name = %s
        """

        result = postgres_hook.get_records(sql, (self.schema, self.table))

        if len(result) == 0:
            raise AirflowException(
                f"""
                No primary key on {self.schema}.{self.table}.
                Please provide keys on 'upsert_keys' parameter.
                """
            )
        return [row[0] for row in result]

    def execute(self, context) -> None:
        self.log.info('Starting execution')
        self.pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info('Downloading S3 file')
        raise AirflowException('No key matches hook', self.s3)
        
#         if self.wildcard_match:
#             if self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
#                 raise AirflowException('No key matches', self.s3_key)
#             s3_key_bucket = self.s3.get_wildcard_key(self.s3_key,
#                                                      self.s3_bucket)
#         else:
#             if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
#                 raise AirflowException("The key {0} does not exist".
#                                        format(self.s3_key))

#         list_content = s3_key_bucket.get()['Body'].read()\
#                                     .decode(encoding='utf-8', errors='ignore')

#         schema = {
#             'producto': 'string',
#             'presentacion': 'string',
#             'marca': 'string',
#             'categoria': 'string',
#             'precio': 'float64',
#             'cadenaComercial': 'string',
#             'giro': 'string',
#             'nombreComercial': 'string',
#             'direccion': 'string',
#             'estado': 'string',
#             'municipio': 'string',
#             'latitud': 'float64',
#             'longitud': 'float64'
#         }

#         date_cols = ['fechaRegistro']

#         df_products = pd.read_csv(io.StringIO(list_content),
#                                   header=0,
#                                   delimiter=',',
#                                   low_memory=False,
#                                   dtype=schema)

#         file_name = 'debootcamp.products.sql'
#         file_path = file_name

#         with open(file_path, "r", encoding="UTF-8") as sql_file:
#             sql_create_table_cmd = sql_file.read()
#             sql_file.close()

#             self.log.info(sql_create_table_cmd)

#         self.pg_hook.run(sql_create_table_cmd)

#         target_fields = ['producto', 'presentacion', 'marca', 'categoria',
#                          'precio', 'cadenaComercial', 'giro', 'nombreComercial',
#                          'direccion', 'estado', 'municipio', 'latitud',
#                          'longitud']

#         self.current_table = self.schema + '.' + self.table
#         self.pg_hook.insert_rows(self.current_table, list_content,
#                                  target_fields=target_fields, commit_every=1000,
#                                  replace=False)
#         self.request = 'SELECT * FROM ' + self.current_table
#         self.connection = self.pg_hook.get_conn()
#         self.cursor = self.connection.cursor()
#         self.cursor.execute(self.request)
#         self.source = self.cursor.fetchall()

#         for source in self.source:
#             self.log.info("producto: {0} - \
#                           presentacion: {1} - \
#                           marca: {2} - \
#                           categoria: {3} - \
#                           catalogo: {4} - \
#                           precio: {5} - \
#                           fechaRegistro: {6} - \
#                           cadenaComercial: {7} - \
#                           giro: {8} - \
#                           nombreComercial: {9} - \
#                           direccion: {10} - \
#                           estado: {11} - \
#                           municipio: {12} - \
#                           latitud: {13} - \
#                           longitud: {14} ".
#                           format(source[0], source[1], source[2], source[3],
#                                  source[4], source[5], source[6], source[7],
#                                  source[8], source[9], source[10], source[11],
#                                  source[12], source[13], source[14]))



