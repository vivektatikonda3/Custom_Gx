from typing import Any, Dict, List, Optional, Union

from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics.import_manager import (
    pyspark_sql_DataFrame,
    pyspark_sql_Row,
    pyspark_sql_SparkSession,
    sa,
    sqlalchemy_engine_Engine,
    sqlalchemy_engine_Row,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
)


class QueryColumn(QueryMetricProvider):
    metric_name = "query.column"
    value_keys = (
        "column",
        "query",
    )

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[sqlalchemy_engine_Row]:
        query: Optional[str] = metric_value_kwargs.get(
            "query"
        ) or cls.default_kwarg_values.get("query")

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        column: str = metric_value_kwargs.get("column")
        if isinstance(selectable, sa.Table):
            query = query.format(col=column, active_batch=selectable)
        elif isinstance(
            selectable, sa.sql.Subquery
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active bacth as a Subquery; sectioning the active batch off w/ parentheses ensures flow of operations doesn't break
            query = query.format(col=column, active_batch=f"({selectable})")
        elif isinstance(
            selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation & aliasing when formatting the parameterized query
            query = query.format(
                col=column,
                active_batch=f'({selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )
        else:
            query = query.format(col=column, active_batch=f"({selectable})")

        engine: sqlalchemy_engine_Engine = execution_engine.engine
        result: List[sqlalchemy_engine_Row] = engine.execute(sa.text(query)).fetchall()

        return result

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[pyspark_sql_Row]:
        query: Optional[str] = metric_value_kwargs.get(
            "query"
        ) or cls.default_kwarg_values.get("query")

        df: pyspark_sql_DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        column: str = metric_value_kwargs.get("column")
        query = query.format(col=column, active_batch="tmp_view")

        engine: pyspark_sql_SparkSession = execution_engine.spark
        result: List[pyspark_sql_Row] = engine.sql(query).collect()

        return result
