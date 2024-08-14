import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    ConcurrencyConfig,
    DataContextConfig,
    NotebookConfig,
    ProgressBarsConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import substitute_config_variable


class DataContextVariableSchema(str, enum.Enum):
    ALL_VARIABLES = (
        "data_context_variables"  # If retrieving/setting the entire config at once
    )
    CONFIG_VERSION = "config_version"
    DATASOURCES = "datasources"
    EXPECTATIONS_STORE_NAME = "expectations_store_name"
    VALIDATIONS_STORE_NAME = "validations_store_name"
    EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store_name"
    CHECKPOINT_STORE_NAME = "checkpoint_store_name"
    PROFILER_STORE_NAME = "profiler_store_name"
    PLUGINS_DIRECTORY = "plugins_directory"
    STORES = "stores"
    DATA_DOCS_SITES = "data_docs_sites"
    NOTEBOOKS = "notebooks"
    CONFIG_VARIABLES_FILE_PATH = "config_variables_file_path"
    ANONYMOUS_USAGE_STATISTICS = "anonymous_usage_statistics"
    CONCURRENCY = "concurrency"
    PROGRESS_BARS = "progress_bars"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """
        Checks whether or not a string is a value from the possible enum pairs.
        """
        return value in cls._value2member_map_


@dataclass
class DataContextVariables(ABC):
    """
    Wrapper object around data context variables set in the `great_expectations.yml` config file.

    Child classes should instantiate their own stores to ensure that changes made to this object
    are persisted for future usage (i.e. filesystem I/O or HTTP request to a Cloud endpoint).

    Should maintain parity with the `DataContextConfig`.

    Args:
        config:        A reference to the DataContextConfig to perform CRUD on.
        substitutions: A dictionary used to perform substitutions of ${VARIABLES}; to be used with GET requests.
        _store:        An instance of a DataContextStore with the appropriate backend to persist config changes.
    """

    config: DataContextConfig
    substitutions: Optional[dict] = None
    _store: Optional["DataContextStore"] = None  # noqa: F821

    def __post_init__(self) -> None:
        if self.substitutions is None:
            self.substitutions = {}

    @property
    def store(self) -> "DataContextStore":  # noqa: F821
        if self._store is None:
            self._store = self._init_store()
        return self._store

    @abstractmethod
    def _init_store(self) -> "DataContextStore":  # noqa: F821
        raise NotImplementedError

    def get_key(self) -> DataContextKey:
        """
        Generates the appropriate Store key to retrieve/store configs.
        """
        key: ConfigurationIdentifier = ConfigurationIdentifier(
            configuration_key=DataContextVariableSchema.ALL_VARIABLES
        )
        return key

    def _set(self, attr: DataContextVariableSchema, value: Any) -> None:
        key: str = attr.value
        self.config[key] = value

    def _get(self, attr: DataContextVariableSchema) -> Any:
        key: str = attr.value
        val: Any = self.config[key]
        substituted_val: Any = substitute_config_variable(val, self.substitutions)
        return substituted_val

    def save_config(self) -> None:
        """
        Persist any changes made to variables utilizing the configured Store.
        """
        key: ConfigurationIdentifier = self.get_key()
        self.store.set(key=key, value=self.config)

    @property
    def config_version(self) -> Optional[float]:
        return self._get(DataContextVariableSchema.CONFIG_VERSION)

    @config_version.setter
    def config_version(self, config_version: float) -> None:
        self._set(DataContextVariableSchema.CONFIG_VERSION, config_version)

    @property
    def config_variables_file_path(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH)

    @config_variables_file_path.setter
    def config_variables_file_path(self, config_variables_file_path: str) -> None:
        self._set(
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            config_variables_file_path,
        )

    @property
    def plugins_directory(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.PLUGINS_DIRECTORY)

    @plugins_directory.setter
    def plugins_directory(self, plugins_directory: str) -> None:
        self._set(DataContextVariableSchema.PLUGINS_DIRECTORY, plugins_directory)

    @property
    def expectations_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.EXPECTATIONS_STORE_NAME)

    @expectations_store_name.setter
    def expectations_store_name(self, expectations_store_name: str) -> None:
        self._set(
            DataContextVariableSchema.EXPECTATIONS_STORE_NAME, expectations_store_name
        )

    @property
    def validations_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.VALIDATIONS_STORE_NAME)

    @validations_store_name.setter
    def validations_store_name(self, validations_store_name: str) -> None:
        self._set(
            DataContextVariableSchema.VALIDATIONS_STORE_NAME, validations_store_name
        )

    @property
    def evaluation_parameter_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME)

    @evaluation_parameter_store_name.setter
    def evaluation_parameter_store_name(
        self, evaluation_parameter_store_name: str
    ) -> None:
        self._set(
            DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
            evaluation_parameter_store_name,
        )

    @property
    def checkpoint_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.CHECKPOINT_STORE_NAME)

    @checkpoint_store_name.setter
    def checkpoint_store_name(self, checkpoint_store_name: str) -> None:
        self._set(
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            checkpoint_store_name,
        )

    @property
    def profiler_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.PROFILER_STORE_NAME)

    @profiler_store_name.setter
    def profiler_store_name(self, profiler_store_name: str) -> None:
        self._set(
            DataContextVariableSchema.PROFILER_STORE_NAME,
            profiler_store_name,
        )

    @property
    def stores(self) -> Optional[dict]:
        return self._get(DataContextVariableSchema.STORES)

    @stores.setter
    def stores(self, stores: dict) -> None:
        self._set(DataContextVariableSchema.STORES, stores)

    @property
    def data_docs_sites(self) -> Optional[dict]:
        return self._get(DataContextVariableSchema.DATA_DOCS_SITES)

    @data_docs_sites.setter
    def data_docs_sites(self, data_docs_sites: dict) -> None:
        self._set(DataContextVariableSchema.DATA_DOCS_SITES, data_docs_sites)

    @property
    def anonymous_usage_statistics(
        self,
    ) -> Optional[AnonymizedUsageStatisticsConfig]:
        return self._get(DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS)

    @anonymous_usage_statistics.setter
    def anonymous_usage_statistics(
        self, anonymous_usage_statistics: AnonymizedUsageStatisticsConfig
    ) -> None:
        self._set(
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
            anonymous_usage_statistics,
        )

    @property
    def notebooks(self) -> Optional[NotebookConfig]:
        return self._get(DataContextVariableSchema.NOTEBOOKS)

    @notebooks.setter
    def notebooks(self, notebooks: NotebookConfig) -> None:
        self._set(
            DataContextVariableSchema.NOTEBOOKS,
            notebooks,
        )

    @property
    def concurrency(self) -> Optional[ConcurrencyConfig]:
        return self._get(DataContextVariableSchema.CONCURRENCY)

    @concurrency.setter
    def concurrency(self, concurrency: ConcurrencyConfig) -> None:
        self._set(
            DataContextVariableSchema.CONCURRENCY,
            concurrency,
        )

    @property
    def progress_bars(self) -> Optional[ProgressBarsConfig]:
        return self._get(DataContextVariableSchema.PROGRESS_BARS)

    @progress_bars.setter
    def progress_bars(self, progress_bars: ProgressBarsConfig) -> None:
        self._set(
            DataContextVariableSchema.PROGRESS_BARS,
            progress_bars,
        )


@dataclass
class EphemeralDataContextVariables(DataContextVariables):
    def _init_store(self) -> "DataContextStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_store import (
            DataContextStore,
        )

        store: DataContextStore = DataContextStore(
            store_name="ephemeral_data_context_store",
            store_backend=None,  # Defaults to InMemoryStoreBackend
            runtime_environment=None,
        )
        return store


@dataclass
class FileDataContextVariables(DataContextVariables):
    data_context: Optional["DataContext"] = None  # noqa: F821

    def __post_init__(self) -> None:
        # Chetan - 20220607 - Although the above argument is not truly optional, we are
        # required to use default values because the parent class defines arguments with default values
        # ("Fields without default values cannot appear after fields with default values").
        #
        # Python 3.10 resolves this issue around dataclass inheritance using `kw_only=True` (https://docs.python.org/3/library/dataclasses.html)
        # This should be modified once our lowest supported version is 3.10.

        if self.data_context is None:
            raise ValueError(
                f"A reference to a data context is required for {self.__class__.__name__}"
            )

    def _init_store(self) -> "DataContextStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_store import (
            DataContextStore,
        )

        store_backend: dict = {
            "class_name": "InlineStoreBackend",
            "data_context": self.data_context,
        }
        store: DataContextStore = DataContextStore(
            store_name="file_data_context_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store


@dataclass
class CloudDataContextVariables(DataContextVariables):
    ge_cloud_base_url: Optional[str] = None
    ge_cloud_organization_id: Optional[str] = None
    ge_cloud_access_token: Optional[str] = None

    def __post_init__(self) -> None:
        # Chetan - 20220607 - Although the above arguments are not truly optional, we are
        # required to use default values because the parent class defines arguments with default values
        # ("Fields without default values cannot appear after fields with default values").
        #
        # Python 3.10 resolves this issue around dataclass inheritance using `kw_only=True` (https://docs.python.org/3/library/dataclasses.html)
        # This should be modified once our lowest supported version is 3.10.

        if any(
            attr is None
            for attr in (
                self.ge_cloud_base_url,
                self.ge_cloud_organization_id,
                self.ge_cloud_access_token,
            )
        ):
            raise ValueError(
                f"All of the following attributes are required for{ self.__class__.__name__}:\n  self.ge_cloud_base_url\n  self.ge_cloud_organization_id\n  self.ge_cloud_access_token"
            )

    def _init_store(self) -> "DataContextStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_store import (
            DataContextStore,
        )

        store_backend: dict = {
            "class_name": "GeCloudStoreBackend",
            "ge_cloud_base_url": self.ge_cloud_base_url,
            "ge_cloud_resource_type": "data_context_variables",
            "ge_cloud_credentials": {
                "access_token": self.ge_cloud_access_token,
                "organization_id": self.ge_cloud_organization_id,
            },
            "suppress_store_backend_id": True,
        }
        store: DataContextStore = DataContextStore(
            store_name="cloud_data_context_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store

    def get_key(self) -> GeCloudIdentifier:
        """
        Generates a GE Cloud-specific key for use with Stores. See parent "DataContextVariables.get_key" for more details.
        """
        key: GeCloudIdentifier = GeCloudIdentifier(
            resource_type=DataContextVariableSchema.ALL_VARIABLES
        )
        return key
