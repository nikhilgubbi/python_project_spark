import logging
import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('properties/configuration/logging.config')

loggers = logging.getLogger("Validate")


def get_current_date(spark):
    try:
        loggers.warning('Started the get_current_date method....')
        output = spark.sql("""select current_date""")
        loggers.warning("Validating spark object with current date" + str(output.collect()))
    except Exception as e:
        loggers.error('An Error occured in get_current_date', str(e))
        raise
    else:
        loggers.warning('Validation completed...go forward')


def print_schema(df, dfName):
    try:
        loggers.warning('Print schema method executing....{}'.format(dfName))

        sch = df.schema.fields

        for i in sch:
            loggers.info(f"\t{i}")

    except Exception as e:
        loggers.error('An error occurred in print_schema...', str(e))

        raise

    else:
        loggers.info('print_schema done, go forward')


def check_for_nulls(df, dfName):
    try:
        loggers.info("check for nulls method executing.......for {}".format(dfName))

        check_null_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

    except Exception as e:
        loggers.error('An error occur while working on check_for_nulls===', str(e))

    else:
        loggers.warning('Check_for_nulls executed successfully...')

    return check_null_df
