"""Custom exceptions for the transform and sampling layers.

Using specific exception types (not generic ValueError) serves two purposes:
1. Airflow tasks can catch DataQualityError vs. SamplingError separately and
   route alerts to different channels (data team vs. analyst team).
2. Tests can assert the exact exception type — making the failure contract
   explicit and preventing silent degradation.

Industry pattern: custom exceptions are standard in production pipelines.
dbt raises dbt.exceptions.DbtRuntimeError; Airflow raises AirflowException.
"""


class DataQualityError(Exception):
    """Raised when a DQ check exceeds the acceptable null or failure rate.

    This is a pipeline-level failure, not a row-level quarantine.
    It means the entire column has too many bad values to proceed safely.

    Analogy: dbt's ``error_if: "> 0"`` severity on a not_null test — the
    pipeline stops rather than silently propagating dirty data downstream.

    Example::

        if null_rate > DQ_MAX_NULL_RATE:
            raise DataQualityError(
                f"gender null rate {null_rate:.1%} exceeds threshold {DQ_MAX_NULL_RATE:.1%}"
            )
    """


class SamplingError(Exception):
    """Raised when the sampling algorithm cannot satisfy its constraints.

    Constraints that trigger this error:
    - Not enough female (or male) candidates in a city-size bucket to fill
      the required per-gender quota.
    - The total sample size cannot reach CANDIDATE_SAMPLE_SIZE.

    Providing a specific exception type (not ValueError) makes it easy for
    an Airflow task to catch sampling failures specifically and alert the
    analyst rather than the data engineering on-call.

    Example::

        if len(female_pool) < target_per_gender:
            raise SamplingError(
                f"Only {len(female_pool)} female candidates in bucket '{bucket}', "
                f"need {target_per_gender}"
            )
    """
