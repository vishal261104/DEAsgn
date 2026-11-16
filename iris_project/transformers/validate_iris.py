try:
    from mage_ai.data_preparation.decorators import transformer, test
except ImportError:
    def identity_decorator(func=None, *args, **kwargs):
        if func is not None and callable(func):
            return func

        def decorator(f):
            return f

        return decorator

    transformer = identity_decorator
    test = identity_decorator

# Expected number of distinct species in the Iris dataset
EXPECTED_CLASS_COUNT = 3


@transformer
def validate_iris(df, *args, **kwargs):
    """Validate the raw Iris DataFrame before it enters the split/train steps.

    Checks performed:
    - No null values anywhere in the DataFrame.
    - Exactly 3 unique target classes (Setosa, Versicolor, Virginica).

    Args:
        df (pd.DataFrame): Raw Iris dataset from load_iris block.

    Returns:
        pd.DataFrame: The unchanged DataFrame if all checks pass.

    Raises:
        AssertionError: If any validation check fails.
    """
    null_counts = df.isnull().sum()
    assert null_counts.sum() == 0, (
        f"Missing values detected:\n{null_counts[null_counts > 0]}"
    )

    n_classes = df["target"].nunique()
    assert n_classes == EXPECTED_CLASS_COUNT, (
        f"Expected {EXPECTED_CLASS_COUNT} target classes, found {n_classes}"
    )

    print(
        f"Validation passed: {len(df)} rows, "
        f"no nulls, {n_classes} target classes."
    )
    return df


@test
def test_validation(output, *args):
    """Confirm the validator returned a clean, correctly-labelled DataFrame."""
    assert not output.isnull().any().any(), "Output contains null values"
    assert output["target"].nunique() == EXPECTED_CLASS_COUNT, (
        "Unexpected number of target classes"
    )
