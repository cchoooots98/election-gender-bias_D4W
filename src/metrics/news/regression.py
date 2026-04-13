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
    "dependent_variable",
    "variable_name",
    "coefficient",
    "std_error",
    "p_value",
    "status",
    "sample_size",
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
# Sparse region dummies are collapsed before bootstrap so repeated resamples do
# not create singular design matrices from singleton regional cells.
_SPARSE_REGION_MIN_OBSERVATIONS = 3
_BOOTSTRAP_PROGRESS_LOG_INTERVAL = 500
_MIN_BOOTSTRAP_SAMPLES_FOR_CI = 50


def _build_regression_design_matrix(modeling_df: pd.DataFrame) -> pd.DataFrame:
    """Build the documented regression controls in one deterministic place."""
    exog_df = pd.DataFrame(
        {
            "const": 1.0,
            "gender_female": modeling_df["gender_female"].astype(float),
            "is_incumbent": modeling_df["is_incumbent"].astype(float),
            "won_final_round": modeling_df["won_final_round"].astype(float),
            "bucket_large": (modeling_df["city_size_bucket"] == "large").astype(float),
            "bucket_medium": (modeling_df["city_size_bucket"] == "medium").astype(
                float
            ),
        }
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
    exog_df: pd.DataFrame,
    status: str,
    sample_size: int,
    fitted_at: str,
) -> pd.DataFrame:
    """Return auditable model rows even when coefficients cannot be estimated."""
    result_rows = []
    for variable_name in exog_df.columns:
        result_rows.append(
            {
                "model_name": model_name,
                "dependent_variable": "article_count",
                "variable_name": variable_name,
                "coefficient": None,
                "std_error": None,
                "p_value": None,
                "status": status,
                "sample_size": sample_size,
                "fitted_at": fitted_at,
            }
        )
    return pd.DataFrame(result_rows, columns=_REGRESSION_RESULT_COLUMNS)


def _fit_count_model(
    *,
    model_name: str,
    endog: pd.Series,
    exog_df: pd.DataFrame,
    offset: pd.Series,
    family: object,
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
            exog_df=exog_df,
            status=f"fit_failed:{type(exc).__name__}",
            sample_size=sample_size,
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
                "dependent_variable": "article_count",
                "variable_name": variable_name,
                "coefficient": float(coefficient),
                "std_error": float(fit_result.bse.get(variable_name, np.nan)),
                "p_value": float(fit_result.pvalues.get(variable_name, np.nan)),
                "status": fit_status,
                "sample_size": sample_size,
                "fitted_at": fitted_at,
            }
        )

    if hasattr(fit_result, "pearson_chi2") and getattr(fit_result, "df_resid", 0) > 0:
        dispersion_ratio = float(fit_result.pearson_chi2 / fit_result.df_resid)
        result_rows.append(
            {
                "model_name": model_name,
                "dependent_variable": "article_count",
                "variable_name": "_dispersion_ratio",
                "coefficient": dispersion_ratio,
                "std_error": None,
                "p_value": None,
                "status": fit_status,
                "sample_size": sample_size,
                "fitted_at": fitted_at,
            }
        )

    return pd.DataFrame(result_rows, columns=_REGRESSION_RESULT_COLUMNS)


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

    modeling_df = regression_feature_base_df.copy()
    exog_df = _build_regression_design_matrix(modeling_df)

    if sm is None:
        return _build_unfitted_regression_rows(
            model_name="poisson_exposure",
            exog_df=exog_df,
            status="not_fitted_missing_statsmodels",
            sample_size=len(modeling_df),
            fitted_at=fitted_at,
        )

    if modeling_df["article_count"].sum() == 0:
        return _build_unfitted_regression_rows(
            model_name="poisson_exposure",
            exog_df=exog_df,
            status="not_fitted_zero_articles",
            sample_size=len(modeling_df),
            fitted_at=fitted_at,
        )

    endog = modeling_df["article_count"].astype(float)
    exposure_offset = np.log(
        modeling_df["population"].clip(lower=_OFFSET_POPULATION_FLOOR).astype(float)
    )

    poisson_df = _fit_count_model(
        model_name="poisson_exposure",
        endog=endog,
        exog_df=exog_df,
        offset=exposure_offset,
        family=sm.families.Poisson(),
        fitted_at=fitted_at,
    )
    negbinom_df = _fit_count_model(
        model_name="negbinom_exposure",
        endog=endog,
        exog_df=exog_df,
        offset=exposure_offset,
        family=sm.families.NegativeBinomial(),
        fitted_at=fitted_at,
    )

    combined_df = pd.concat([poisson_df, negbinom_df], ignore_index=True)
    logger.info(
        "Regression diagnostics built poisson_rows=%d negbinom_rows=%d",
        len(poisson_df),
        len(negbinom_df),
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

    modeling_df = regression_feature_base_df.copy()

    reg_counts = modeling_df["reg_code"].value_counts()
    sparse_regions = set(
        reg_counts[reg_counts < _SPARSE_REGION_MIN_OBSERVATIONS].index.astype(str)
    )
    if sparse_regions:
        modeling_df = modeling_df.copy()
        modeling_df["reg_code"] = modeling_df["reg_code"].apply(
            lambda region_code: (
                "other" if str(region_code) in sparse_regions else region_code
            )
        )
        logger.info(
            "Bootstrap collapsed sparse regions into reg_other regions=%s",
            sorted(sparse_regions),
        )

    exog_df = _build_regression_design_matrix(modeling_df)
    endog = modeling_df["article_count"].astype(float)
    offset = np.log(
        modeling_df["population"].clip(lower=_OFFSET_POPULATION_FLOOR).astype(float)
    )
    variable_names = exog_df.columns.tolist()
    n_obs = len(modeling_df)

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
