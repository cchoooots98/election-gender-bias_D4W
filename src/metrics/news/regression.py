"""Regression diagnostics for the news exposure analysis."""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from numpy.linalg import LinAlgError

try:
    import statsmodels.api as sm
except ImportError:  # pragma: no cover - depends on local environment
    sm = None

try:
    from statsmodels.tools.sm_exceptions import PerfectSeparationError
except ImportError:  # pragma: no cover - depends on local environment
    PerfectSeparationError = None

logger = logging.getLogger(__name__)

_REGRESSION_FIT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    FloatingPointError,
    LinAlgError,
    RuntimeError,
    ValueError,
)
if PerfectSeparationError is not None:
    _REGRESSION_FIT_EXCEPTIONS = _REGRESSION_FIT_EXCEPTIONS + (PerfectSeparationError,)

_REGRESSION_RESULT_COLUMNS = [
    "model_name",
    "model_role",
    "dependent_variable",
    "variable_name",
    "coefficient",
    "std_error",
    "p_value",
    "q_value",
    "status",
    "inference_status",
    "is_publishable",
    "sample_size",
    "parameter_count",
    "excluded_missing_control_count",
    "fitted_at",
]
_BOOTSTRAP_CI_COLUMNS = [
    "variable_name",
    "n_bootstrap",
    "n_converged",
    "observed_coef",
    "ci_lower_95",
    "ci_upper_95",
    "ci_lower_90",
    "ci_upper_90",
    "bootstrap_std",
    "ci_excludes_zero",
    "fitted_at",
]
# Population is the log-offset denominator; floor at 1 to keep zero-population
# edge cases auditable without producing an infinite offset.
_OFFSET_POPULATION_FLOOR = 1
_BOOTSTRAP_PROGRESS_LOG_INTERVAL = 500
_MIN_BOOTSTRAP_SAMPLES_FOR_CI = 50
_PLACEBO_RANDOM_SEED = 20260527
_PRIMARY_MODEL_ROLE = "Primary model"
_DIAGNOSTIC_MODEL_ROLE = "Diagnostic only"
_SENSITIVITY_MODEL_ROLE = "Sensitivity model"
_PLACEBO_MODEL_ROLE = "Placebo check"


def _build_primary_design_matrix(modeling_df: pd.DataFrame) -> pd.DataFrame:
    """Build the low-dimensional primary exposure model design matrix."""
    return pd.DataFrame(
        {
            "const": 1.0,
            "gender_female": modeling_df["gender_female"].astype(float),
            "is_incumbent": modeling_df["is_incumbent"].astype(float),
        }
    )


def _build_placebo_design_matrix(modeling_df: pd.DataFrame) -> pd.DataFrame:
    """Build the fixed-seed placebo design matrix."""
    rng = np.random.default_rng(_PLACEBO_RANDOM_SEED)
    placebo_gender = rng.permutation(
        modeling_df["gender_female"].astype(int).to_numpy()
    )
    return pd.DataFrame(
        {
            "const": 1.0,
            "gender_female_placebo": placebo_gender.astype(float),
            "is_incumbent": modeling_df["is_incumbent"].astype(float),
        }
    )


def _build_sensitivity_design_matrix(modeling_df: pd.DataFrame) -> pd.DataFrame:
    """Build the appendix full-control design matrix."""
    exog_df = _build_primary_design_matrix(modeling_df)
    exog_df["won_final_round"] = modeling_df["won_final_round"].astype(float)
    exog_df["bucket_large"] = (modeling_df["city_size_bucket"] == "large").astype(float)
    exog_df["bucket_medium"] = (modeling_df["city_size_bucket"] == "medium").astype(
        float
    )

    nuance_categories = sorted(
        {
            str(value)
            for value in modeling_df["nuance_group"].dropna().astype(str).tolist()
            if str(value).strip()
        }
    )
    if len(nuance_categories) > 1:
        nuance_dummies = pd.get_dummies(
            pd.Categorical(modeling_df["nuance_group"], categories=nuance_categories),
            prefix="nuance_group",
            drop_first=True,
            dtype=float,
        )
        exog_df = pd.concat([exog_df, nuance_dummies], axis=1)

    region_categories = sorted(
        {
            str(value)
            for value in modeling_df["reg_code"].dropna().astype(str).tolist()
            if str(value).strip()
        }
    )
    if len(region_categories) > 1:
        region_dummies = pd.get_dummies(
            pd.Categorical(modeling_df["reg_code"], categories=region_categories),
            prefix="reg_code",
            drop_first=True,
            dtype=float,
        )
        exog_df = pd.concat([exog_df, region_dummies], axis=1)

    return exog_df


def _build_unfitted_regression_rows(
    *,
    model_name: str,
    model_role: str,
    exog_df: pd.DataFrame,
    status: str,
    sample_size: int,
    excluded_missing_control_count: int,
    fitted_at: str,
) -> pd.DataFrame:
    """Return auditable model rows even when coefficients cannot be estimated."""
    result_rows = []
    for variable_name in exog_df.columns:
        result_rows.append(
            {
                "model_name": model_name,
                "model_role": model_role,
                "dependent_variable": "article_count",
                "variable_name": variable_name,
                "coefficient": None,
                "std_error": None,
                "p_value": None,
                "q_value": None,
                "status": status,
                "inference_status": "not_fitted",
                "is_publishable": False,
                "sample_size": sample_size,
                "parameter_count": len(exog_df.columns),
                "excluded_missing_control_count": excluded_missing_control_count,
                "fitted_at": fitted_at,
            }
        )
    return pd.DataFrame(result_rows, columns=_REGRESSION_RESULT_COLUMNS)


def _build_unfitted_regression_result_set(
    *,
    modeling_df: pd.DataFrame,
    regression_feature_base_df: pd.DataFrame,
    status: str,
    excluded_missing_control_count: int,
    fitted_at: str,
) -> pd.DataFrame:
    """Return all planned model rows when fitting cannot run."""
    primary_exog_df = _build_primary_design_matrix(modeling_df)
    result_frames = [
        _build_unfitted_regression_rows(
            model_name="poisson_exposure",
            model_role=_DIAGNOSTIC_MODEL_ROLE,
            exog_df=primary_exog_df,
            status=status,
            sample_size=len(modeling_df),
            excluded_missing_control_count=excluded_missing_control_count,
            fitted_at=fitted_at,
        ),
        _build_unfitted_regression_rows(
            model_name="negbinom_exposure",
            model_role=_PRIMARY_MODEL_ROLE,
            exog_df=primary_exog_df,
            status=status,
            sample_size=len(modeling_df),
            excluded_missing_control_count=excluded_missing_control_count,
            fitted_at=fitted_at,
        ),
        _build_unfitted_regression_rows(
            model_name="negbinom_exposure_placebo",
            model_role=_PLACEBO_MODEL_ROLE,
            exog_df=_build_placebo_design_matrix(modeling_df),
            status=status,
            sample_size=len(modeling_df),
            excluded_missing_control_count=excluded_missing_control_count,
            fitted_at=fitted_at,
        ),
    ]

    sensitivity_required_columns = (
        "article_count",
        "population",
        "gender_female",
        "is_incumbent",
        "won_final_round",
        "city_size_bucket",
        "nuance_group",
        "reg_code",
    )
    sensitivity_modeling_df, sensitivity_excluded_count = _prepare_modeling_dataframe(
        regression_feature_base_df,
        required_columns=sensitivity_required_columns,
    )
    if set(sensitivity_required_columns).issubset(regression_feature_base_df.columns):
        result_frames.append(
            _build_unfitted_regression_rows(
                model_name="negbinom_exposure_full_controls",
                model_role=_SENSITIVITY_MODEL_ROLE,
                exog_df=_build_sensitivity_design_matrix(sensitivity_modeling_df),
                status=status,
                sample_size=len(sensitivity_modeling_df),
                excluded_missing_control_count=sensitivity_excluded_count,
                fitted_at=fitted_at,
            )
        )

    return pd.concat(result_frames, ignore_index=True)[_REGRESSION_RESULT_COLUMNS]


def _fit_count_model(
    *,
    model_name: str,
    model_role: str,
    endog: pd.Series,
    exog_df: pd.DataFrame,
    offset: pd.Series,
    family: object,
    excluded_missing_control_count: int,
    fitted_at: str,
) -> pd.DataFrame:
    """Fit one generalized linear count model and return tidy results."""
    sample_size = len(endog)
    try:
        model = sm.GLM(endog, exog_df, family=family, offset=offset)
        with warnings.catch_warnings(record=True) as fit_warnings:
            warnings.simplefilter("always")
            fit_result = model.fit()
    except _REGRESSION_FIT_EXCEPTIONS as exc:
        logger.warning("Model fit failed model=%s error=%r", model_name, exc)
        return _build_unfitted_regression_rows(
            model_name=model_name,
            model_role=model_role,
            exog_df=exog_df,
            status=f"fit_failed:{type(exc).__name__}",
            sample_size=sample_size,
            excluded_missing_control_count=excluded_missing_control_count,
            fitted_at=fitted_at,
        )

    warning_types = sorted({type(warning.message).__name__ for warning in fit_warnings})
    fit_status = (
        "fitted"
        if not warning_types
        else f"fitted_with_warning:{','.join(warning_types)}"
    )
    if warning_types:
        logger.warning(
            "Model fit completed with warnings model=%s types=%s",
            model_name,
            warning_types,
        )

    result_rows = []
    for variable_name, coefficient in fit_result.params.items():
        result_rows.append(
            {
                "model_name": model_name,
                "model_role": model_role,
                "dependent_variable": "article_count",
                "variable_name": variable_name,
                "coefficient": float(coefficient),
                "std_error": float(fit_result.bse.get(variable_name, np.nan)),
                "p_value": float(fit_result.pvalues.get(variable_name, np.nan)),
                "q_value": None,
                "status": fit_status,
                "inference_status": "pending_q_value",
                "is_publishable": False,
                "sample_size": sample_size,
                "parameter_count": len(exog_df.columns),
                "excluded_missing_control_count": excluded_missing_control_count,
                "fitted_at": fitted_at,
            }
        )

    if hasattr(fit_result, "pearson_chi2") and getattr(fit_result, "df_resid", 0) > 0:
        dispersion_ratio = float(fit_result.pearson_chi2 / fit_result.df_resid)
        result_rows.append(
            {
                "model_name": model_name,
                "model_role": model_role,
                "dependent_variable": "article_count",
                "variable_name": "_dispersion_ratio",
                "coefficient": dispersion_ratio,
                "std_error": None,
                "p_value": None,
                "q_value": None,
                "status": fit_status,
                "inference_status": "diagnostic",
                "is_publishable": False,
                "sample_size": sample_size,
                "parameter_count": len(exog_df.columns),
                "excluded_missing_control_count": excluded_missing_control_count,
                "fitted_at": fitted_at,
            }
        )

    return pd.DataFrame(result_rows, columns=_REGRESSION_RESULT_COLUMNS)


def _prepare_modeling_dataframe(
    regression_feature_base_df: pd.DataFrame,
    *,
    required_columns: tuple[str, ...],
) -> tuple[pd.DataFrame, int]:
    """Drop rows with missing required modeling controls and return the count."""
    modeling_df = regression_feature_base_df.copy()
    missing_required = modeling_df.loc[:, list(required_columns)].isna().any(axis=1)
    excluded_count = int(missing_required.sum())
    if excluded_count:
        logger.warning(
            "Regression excluded rows with missing controls count=%d columns=%s",
            excluded_count,
            required_columns,
        )
    return modeling_df.loc[~missing_required].reset_index(drop=True), excluded_count


def _population_offset(modeling_df: pd.DataFrame) -> pd.Series:
    """Return the log-population offset used by exposure count models."""
    return np.log(
        modeling_df["population"].clip(lower=_OFFSET_POPULATION_FLOOR).astype(float)
    )


def _apply_benjamini_hochberg_q_values(regression_df: pd.DataFrame) -> pd.DataFrame:
    """Add Benjamini-Hochberg q-values to fitted coefficient rows."""
    if regression_df.empty:
        return regression_df
    result_df = regression_df.copy()
    result_df["q_value"] = None
    p_value_series = pd.to_numeric(result_df["p_value"], errors="coerce")
    valid_mask = p_value_series.notna()
    if not valid_mask.any():
        return result_df[_REGRESSION_RESULT_COLUMNS]

    valid_p_values = p_value_series.loc[valid_mask].astype(float)
    ordered_indices = valid_p_values.sort_values(ascending=False).index.tolist()
    total_tests = len(valid_p_values)
    running_min = 1.0
    q_values_by_index: dict[object, float] = {}
    for reverse_rank, row_index in enumerate(ordered_indices, start=1):
        rank = total_tests - reverse_rank + 1
        adjusted_value = float(valid_p_values.loc[row_index]) * total_tests / rank
        running_min = min(running_min, adjusted_value)
        q_values_by_index[row_index] = min(running_min, 1.0)
    for row_index, q_value in q_values_by_index.items():
        result_df.at[row_index, "q_value"] = q_value
    return result_df[_REGRESSION_RESULT_COLUMNS]


def _apply_regression_publishability_flags(
    regression_df: pd.DataFrame,
) -> pd.DataFrame:
    """Add machine-readable inference status for downstream governance."""
    if regression_df.empty:
        return regression_df
    result_df = regression_df.copy()
    result_df["inference_status"] = "not_publishable"
    result_df["is_publishable"] = False

    fitted_mask = result_df["status"].astype(str).str.startswith("fitted")
    diagnostic_mask = result_df["variable_name"].astype(str).eq("_dispersion_ratio")
    intercept_mask = result_df["variable_name"].astype(str).eq("const")
    poisson_mask = result_df["model_name"].astype(str).eq("poisson_exposure")
    placebo_mask = result_df["model_name"].astype(str).eq("negbinom_exposure_placebo")
    q_value_series = pd.to_numeric(result_df["q_value"], errors="coerce")
    coefficient_mask = (
        fitted_mask
        & ~diagnostic_mask
        & ~intercept_mask
        & ~poisson_mask
        & ~placebo_mask
        & q_value_series.notna()
    )

    result_df.loc[diagnostic_mask, "inference_status"] = "diagnostic"
    result_df.loc[intercept_mask & fitted_mask, "inference_status"] = "intercept"
    result_df.loc[poisson_mask & fitted_mask, "inference_status"] = "diagnostic_only"
    result_df.loc[placebo_mask & fitted_mask, "inference_status"] = "placebo_check"
    result_df.loc[coefficient_mask, "inference_status"] = "inconclusive"

    publishable_mask = coefficient_mask & q_value_series.le(0.05)
    result_df.loc[publishable_mask, "inference_status"] = "publishable_signal"
    result_df.loc[publishable_mask, "is_publishable"] = True
    return result_df[_REGRESSION_RESULT_COLUMNS]


def build_mart_regression_results(
    regression_feature_base_df: pd.DataFrame,
) -> pd.DataFrame:
    """Fit Poisson and Negative Binomial exposure diagnostics.

    Args:
        regression_feature_base_df: One-row-per-leader modeling base produced
            by dbt from ``gold.mart_regression_feature_base``.

    Returns:
        DataFrame with one row per model coefficient and diagnostic. The
        dependent variable is ``article_count`` with a log-population offset.
    """
    fitted_at = datetime.now(UTC).isoformat()
    if regression_feature_base_df.empty:
        return pd.DataFrame(columns=_REGRESSION_RESULT_COLUMNS)

    primary_required_columns = (
        "article_count",
        "population",
        "gender_female",
        "is_incumbent",
    )
    modeling_df, excluded_count = _prepare_modeling_dataframe(
        regression_feature_base_df,
        required_columns=primary_required_columns,
    )
    exog_df = _build_primary_design_matrix(modeling_df)

    if sm is None:
        return _build_unfitted_regression_result_set(
            modeling_df=modeling_df,
            regression_feature_base_df=regression_feature_base_df,
            status="not_fitted_missing_statsmodels",
            excluded_missing_control_count=excluded_count,
            fitted_at=fitted_at,
        )

    if modeling_df.empty or modeling_df["article_count"].sum() == 0:
        return _build_unfitted_regression_result_set(
            modeling_df=modeling_df,
            regression_feature_base_df=regression_feature_base_df,
            status="not_fitted_zero_articles",
            excluded_missing_control_count=excluded_count,
            fitted_at=fitted_at,
        )

    endog = modeling_df["article_count"].astype(float)
    exposure_offset = _population_offset(modeling_df)

    poisson_df = _fit_count_model(
        model_name="poisson_exposure",
        model_role=_DIAGNOSTIC_MODEL_ROLE,
        endog=endog,
        exog_df=exog_df,
        offset=exposure_offset,
        family=sm.families.Poisson(),
        excluded_missing_control_count=excluded_count,
        fitted_at=fitted_at,
    )
    negbinom_df = _fit_count_model(
        model_name="negbinom_exposure",
        model_role=_PRIMARY_MODEL_ROLE,
        endog=endog,
        exog_df=exog_df,
        offset=exposure_offset,
        family=sm.families.NegativeBinomial(),
        excluded_missing_control_count=excluded_count,
        fitted_at=fitted_at,
    )
    placebo_df = _fit_count_model(
        model_name="negbinom_exposure_placebo",
        model_role=_PLACEBO_MODEL_ROLE,
        endog=endog,
        exog_df=_build_placebo_design_matrix(modeling_df),
        offset=exposure_offset,
        family=sm.families.NegativeBinomial(),
        excluded_missing_control_count=excluded_count,
        fitted_at=fitted_at,
    )
    sensitivity_required_columns = (
        *primary_required_columns,
        "won_final_round",
        "city_size_bucket",
        "nuance_group",
        "reg_code",
    )
    sensitivity_modeling_df, sensitivity_excluded_count = _prepare_modeling_dataframe(
        regression_feature_base_df,
        required_columns=sensitivity_required_columns,
    )
    if sensitivity_modeling_df.empty:
        sensitivity_df = pd.DataFrame(columns=_REGRESSION_RESULT_COLUMNS)
    else:
        sensitivity_df = _fit_count_model(
            model_name="negbinom_exposure_full_controls",
            model_role=_SENSITIVITY_MODEL_ROLE,
            endog=sensitivity_modeling_df["article_count"].astype(float),
            exog_df=_build_sensitivity_design_matrix(sensitivity_modeling_df),
            offset=_population_offset(sensitivity_modeling_df),
            family=sm.families.NegativeBinomial(),
            excluded_missing_control_count=sensitivity_excluded_count,
            fitted_at=fitted_at,
        )

    combined_df = pd.concat(
        [poisson_df, negbinom_df, placebo_df, sensitivity_df],
        ignore_index=True,
    )
    combined_df = _apply_benjamini_hochberg_q_values(combined_df)
    combined_df = _apply_regression_publishability_flags(combined_df)
    logger.info(
        "Regression diagnostics built poisson_rows=%d negbinom_rows=%d "
        "placebo_rows=%d sensitivity_rows=%d",
        len(poisson_df),
        len(negbinom_df),
        len(placebo_df),
        len(sensitivity_df),
    )
    return combined_df[_REGRESSION_RESULT_COLUMNS].copy()


def build_mart_bootstrap_ci(
    regression_feature_base_df: pd.DataFrame,
    n_bootstrap: int = 2000,
    random_seed: int = 42,
) -> pd.DataFrame:
    """Estimate bootstrap confidence intervals for exposure coefficients.

    Args:
        regression_feature_base_df: One-row-per-leader modeling base produced
            by dbt from ``gold.mart_regression_feature_base``.
        n_bootstrap: Number of bootstrap resamples.
        random_seed: Seed for reproducible re-runs.

    Returns:
        DataFrame with confidence intervals by variable. ``n_converged`` lower
        than ``n_bootstrap`` means some resamples were numerically unstable.
    """
    fitted_at = datetime.now(UTC).isoformat()

    if sm is None or regression_feature_base_df.empty:
        logger.warning(
            "Bootstrap CI skipped: sm=%s empty=%s",
            sm is None,
            regression_feature_base_df.empty,
        )
        return pd.DataFrame(columns=_BOOTSTRAP_CI_COLUMNS)

    modeling_df, excluded_count = _prepare_modeling_dataframe(
        regression_feature_base_df,
        required_columns=(
            "article_count",
            "population",
            "gender_female",
            "is_incumbent",
        ),
    )
    if modeling_df.empty:
        logger.warning(
            "Bootstrap CI skipped: all rows excluded by missing primary controls"
        )
        return pd.DataFrame(columns=_BOOTSTRAP_CI_COLUMNS)

    exog_df = _build_primary_design_matrix(modeling_df)
    endog = modeling_df["article_count"].astype(float)
    offset = _population_offset(modeling_df)
    variable_names = exog_df.columns.tolist()
    n_obs = len(modeling_df)
    if excluded_count:
        logger.info("Bootstrap excluded missing-control rows count=%d", excluded_count)

    try:
        observed_result = sm.GLM(
            endog, exog_df, family=sm.families.NegativeBinomial(), offset=offset
        ).fit(disp=False)
        observed_coefs: dict[str, float] = observed_result.params.to_dict()
    except _REGRESSION_FIT_EXCEPTIONS as exc:
        logger.warning("Bootstrap observed fit failed error=%r", exc)
        return pd.DataFrame(columns=_BOOTSTRAP_CI_COLUMNS)

    rng = np.random.default_rng(seed=random_seed)
    resample_coefs: dict[str, list[float]] = {
        variable: [] for variable in variable_names
    }
    n_converged = 0

    for bootstrap_index in range(n_bootstrap):
        resample_idx = rng.integers(low=0, high=n_obs, size=n_obs)
        endog_resample = endog.iloc[resample_idx].reset_index(drop=True)
        exog_resample = exog_df.iloc[resample_idx].reset_index(drop=True)
        offset_resample = offset.iloc[resample_idx].reset_index(drop=True)

        non_const_columns = [
            column for column in exog_resample.columns if column != "const"
        ]
        if (exog_resample[non_const_columns].std() == 0).any():
            logger.debug(
                "Bootstrap resample skipped due to zero-variance predictor "
                "bootstrap_index=%d",
                bootstrap_index,
            )
            continue

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = sm.GLM(
                    endog_resample,
                    exog_resample,
                    family=sm.families.NegativeBinomial(),
                    offset=offset_resample,
                ).fit(disp=False)
        except _REGRESSION_FIT_EXCEPTIONS as exc:
            logger.debug(
                "Bootstrap resample fit failed bootstrap_index=%d error=%r",
                bootstrap_index,
                exc,
            )
            continue

        n_converged += 1
        for variable_name, coefficient in result.params.items():
            if variable_name in resample_coefs:
                resample_coefs[variable_name].append(float(coefficient))

        if (bootstrap_index + 1) % _BOOTSTRAP_PROGRESS_LOG_INTERVAL == 0:
            logger.info(
                "Bootstrap progress: %d/%d resamples converged=%d",
                bootstrap_index + 1,
                n_bootstrap,
                n_converged,
            )

    logger.info(
        "Bootstrap complete: n_bootstrap=%d n_converged=%d convergence_rate=%.1f%%",
        n_bootstrap,
        n_converged,
        100 * n_converged / n_bootstrap,
    )

    rows = []
    for variable_name in variable_names:
        coefficient_samples = resample_coefs[variable_name]
        observed = observed_coefs.get(variable_name, float("nan"))

        if len(coefficient_samples) < _MIN_BOOTSTRAP_SAMPLES_FOR_CI:
            rows.append(
                {
                    "variable_name": variable_name,
                    "n_bootstrap": n_bootstrap,
                    "n_converged": n_converged,
                    "observed_coef": observed,
                    "ci_lower_95": float("nan"),
                    "ci_upper_95": float("nan"),
                    "ci_lower_90": float("nan"),
                    "ci_upper_90": float("nan"),
                    "bootstrap_std": float("nan"),
                    "ci_excludes_zero": False,
                    "fitted_at": fitted_at,
                }
            )
            continue

        samples_arr = np.array(coefficient_samples)
        ci_lower_95, ci_upper_95 = np.percentile(samples_arr, [2.5, 97.5])
        ci_lower_90, ci_upper_90 = np.percentile(samples_arr, [5.0, 95.0])

        rows.append(
            {
                "variable_name": variable_name,
                "n_bootstrap": n_bootstrap,
                "n_converged": n_converged,
                "observed_coef": observed,
                "ci_lower_95": float(ci_lower_95),
                "ci_upper_95": float(ci_upper_95),
                "ci_lower_90": float(ci_lower_90),
                "ci_upper_90": float(ci_upper_90),
                "bootstrap_std": float(samples_arr.std()),
                "ci_excludes_zero": bool(ci_lower_95 > 0 or ci_upper_95 < 0),
                "fitted_at": fitted_at,
            }
        )

    return pd.DataFrame(rows, columns=_BOOTSTRAP_CI_COLUMNS)
