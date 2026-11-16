import pandas as pd
from sklearn.datasets import load_iris

try:
    from mage_ai.data_preparation.decorators import data_loader, test
except ImportError:
    def identity_decorator(func=None, *args, **kwargs):
        if func is not None and callable(func):
            return func

        def decorator(f):
            return f

        return decorator

    data_loader = identity_decorator
    test = identity_decorator

# Expected number of samples in the Iris dataset
EXPECTED_ROW_COUNT = 150


@data_loader
def load_data(*args, **kwargs):
    """Load the Iris dataset from scikit-learn and return it as a DataFrame.

    Returns:
        pd.DataFrame: 150 rows with 4 feature columns and 1 target column.
    """
    iris = load_iris(as_frame=True)
    df = iris.frame  # 4 feature columns + 'target'
    print("Rows loaded:", len(df))
    return df


@test
def test_output(output, *args):
    """Verify the dataset has exactly 150 rows."""
    assert len(output) == EXPECTED_ROW_COUNT, (
        f"Expected {EXPECTED_ROW_COUNT} rows, got {len(output)}"
    )
