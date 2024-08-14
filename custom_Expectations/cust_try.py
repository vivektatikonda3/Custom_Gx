from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation, InvalidExpectationConfigurationError
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial
from great_expectations.expectations.registry import register_expectation
import pandas as pd
from sqlalchemy import func

# Define the custom metric for freshness
class ColumnValuesWithinFreshness(ColumnMapMetricProvider):
    condition_metric_name = "column_values.within_freshness"
    condition_value_keys = ("freshness_days",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        freshness_days = kwargs.get("freshness_days", 30)
        
        # Calculate the latest allowed date (30 days before now)
        latest_date = pd.Timestamp.now().normalize() - pd.to_timedelta(freshness_days, unit="d")

        # Ensure the column and latest_date are both timezone-naive
        if column.dt.tz is not None:  # If column is timezone-aware, make latest_date timezone-aware
            latest_date = latest_date.tz_localize(column.dt.tz)
        else:  # Column is timezone-naive
            latest_date = latest_date.tz_localize(None)

        # Debugging: Print the latest_date and the first few dates in the column
        print(f"Debug - Latest date for comparison: {latest_date}")
        print(f"Debug - First few dates in the column: {column.head()}")

        # Perform the comparison and print the result
        comparison_result = column >= latest_date
        print(f"Debug - Comparison result: {comparison_result.head()}")

        return comparison_result


    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        freshness_days = kwargs.get("freshness_days", 30)
        latest_date = func.now() - func.cast(freshness_days, func.INTERVAL)
        return column >= latest_date

# Define the custom expectation for freshness
class ExpectColumnValuesToBeWithinFreshnessExpectation(ColumnMapExpectation):
    map_metric = "column_values.within_freshness"
    success_keys = ("freshness_days",)

    default_kwarg_values = {
        "freshness_days": 30,
    }

    def validate_configuration(self, configuration: ExpectationConfiguration):
        super().validate_configuration(configuration)
        if "freshness_days" not in configuration.kwargs:
            raise InvalidExpectationConfigurationError("freshness_days must be provided")
        return True

# Register the expectation
register_expectation(ExpectColumnValuesToBeWithinFreshnessExpectation)


































# import pandas as pd
# from great_expectations.core.expectation_configuration import ExpectationConfiguration
# from great_expectations.execution_engine import PandasExecutionEngine, SqlAlchemyExecutionEngine
# from great_expectations.expectations.expectation import ColumnMapExpectation, InvalidExpectationConfigurationError
# from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial
# from great_expectations.expectations.registry import register_metric, register_expectation
# from sqlalchemy import func

# # Define the custom metric for freshness
# class ColumnValuesWithinFreshness(ColumnMapMetricProvider):
#     condition_metric_name = "column_values.within_freshness"
#     condition_value_keys = ("freshness_days",)

#     @column_condition_partial(engine=PandasExecutionEngine)
#     def _pandas(cls, column, **kwargs):
#         freshness_days = kwargs.get("freshness_days", 30)
#         latest_date = pd.Timestamp.now() - pd.to_timedelta(freshness_days, unit="d")
#         return column >= latest_date

#     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#     def _sqlalchemy(cls, column, **kwargs):
#         freshness_days = kwargs.get("freshness_days", 30)
#         latest_date = func.now() - func.cast(freshness_days, func.INTERVAL)
#         return column >= latest_date

# # Define the custom expectation for freshness
# class ExpectColumnValuesToBeWithinFreshnessExpectation(ColumnMapExpectation):
#     map_metric = "column_values.within_freshness"
#     success_keys = ("freshness_days",)

#     default_kwarg_values = {
#         "freshness_days": 30,
#     }

#     def validate_configuration(self, configuration: ExpectationConfiguration):
#         super().validate_configuration(configuration)
#         if "freshness_days" not in configuration.kwargs:
#             raise InvalidExpectationConfigurationError("freshness_days must be provided")
#         return True

# # Ensure that the metric is registered for both Pandas and SqlAlchemy
# register_metric(
#     metric_name=ColumnValuesWithinFreshness.condition_metric_name,
#     metric_domain_keys=("column",),
#     metric_value_keys=("freshness_days",),
#     execution_engine=PandasExecutionEngine,
#     metric_class=ColumnValuesWithinFreshness,
#     metric_provider=[ColumnValuesWithinFreshness._pandas]
# )

# register_metric(
#     metric_name=ColumnValuesWithinFreshness.condition_metric_name,
#     metric_domain_keys=("column",),
#     metric_value_keys=("freshness_days",),
#     execution_engine=SqlAlchemyExecutionEngine,
#     metric_class=ColumnValuesWithinFreshness,
#     metric_provider=[ColumnValuesWithinFreshness._sqlalchemy]
# )

# # Register the expectation
# register_expectation(ExpectColumnValuesToBeWithinFreshnessExpectation)
