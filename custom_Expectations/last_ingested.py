import pandas as pd
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial
from great_expectations.expectations.expectation import ColumnMapExpectation, InvalidExpectationConfigurationError
from great_expectations.expectations.registry import register_expectation

class ExpectColumnValuesToBeWithinFreshness(ColumnMapMetricProvider):
    condition_metric_name = "column_values.within_freshness"
    
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        freshness_days = kwargs.get("freshness_days", 30)
        latest_date = pd.Timestamp.now() - pd.to_timedelta(freshness_days, unit="d")
        return column >= latest_date

class ExpectColumnValuesToBeWithinFreshnessExpectation(ColumnMapExpectation):
    map_metric = "column_values.within_freshness"
    success_keys = ("freshness_days",)

    default_kwarg_values = {
        "freshness_days": 30,
    }

    def validate_configuration(self, configuration):
        super().validate_configuration(configuration)
        if "freshness_days" not in configuration.kwargs:
            raise InvalidExpectationConfigurationError("freshness_days must be provided")

# Register the custom expectation
register_expectation(ExpectColumnValuesToBeWithinFreshnessExpectation)
