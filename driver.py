import os
import sys

import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date, print_schema
from ingest import load_files, display_df, df_count
from data_processing import *
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_format, file_dir, header, inferSchema, file_dir2
    try:
        logging.info('i am in the main method..')
        # print(gav.header)
        # print(gav.src_olap)
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('Validating spark object..........')
        get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            print("File is :", file)

            file_dir = gav.src_olap + '/' + file
            print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        logging.info("displaying file")
        display_df(df_city, 'df_city')

        logging.info("here to validate the df")

        df_count(df_city, 'df_city')

        logging.info('checking for the files in the Fact...')

        for files in os.listdir(gav.src_oltp):
            print("Src Files::" + files)

            file_dir = gav.src_oltp + '/' + files
            print(file_dir)

            if files.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif files.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        logging.info('displaying the df_fact dataframe')
        display_df(df_fact, 'df_fact')

        # validate::
        df_count(df_fact, 'df_fact')

        logging.info("implementing data_processing methods...")

        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)

        display_df(df_city_sel, 'df_city')

        display_df(df_presc_sel,'df_fact')

        logging.info('Validating schema for the dataframes....')

        print_schema(df_city_sel, 'df_city_sel')

        print_schema(df_presc_sel, 'df_presc_sel')



    except Exception as exp:
        logging.error("An error when calling main() Please check the trace====", str(exp))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application Completed...')

