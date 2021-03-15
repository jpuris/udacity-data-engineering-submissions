import helpers
import operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    """
    Sparkify plugin...
    """
    name = 'sparkify_plugin'
    operators = [
        operators.CreateTableOperator,
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
    ]
    helpers = [
        helpers.SqlQueries,
    ]
