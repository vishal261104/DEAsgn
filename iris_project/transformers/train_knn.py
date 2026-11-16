from sklearn.neighbors import KNeighborsClassifier

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

FEATURES = [
    "sepal length (cm)",
    "sepal width (cm)",
    "petal length (cm)",
    "petal width (cm)",
]

# Default number of neighbours for the KNN classifier
DEFAULT_K = 5

# Minimum acceptable test accuracy
MIN_ACCURACY = 0.8


@transformer
def train_knn(df, *args, **kwargs):
    """Train a K-Nearest Neighbours classifier and return predictions.

    Uses the same 70/30 train/test split produced by prepare_data.
    The number of neighbours is read from the pipeline variable `k`
    (defaults to 5 if not set).

    Args:
        df (pd.DataFrame): Combined train/test DataFrame with a 'split' column.

    Returns:
        dict: A dict with key 'accuracy' (float) for downstream quality checks.
    """
    k = int(kwargs.get("k", DEFAULT_K))

    train_df = df[df["split"] == "train"]
    test_df = df[df["split"] == "test"].copy()

    model = KNeighborsClassifier(n_neighbors=k)
    model.fit(train_df[FEATURES], train_df["target"])

    test_df["predicted"] = model.predict(test_df[FEATURES])
    accuracy = (test_df["predicted"] == test_df["target"]).mean()

    print(f"KNN (k={k}) | Test accuracy = {round(accuracy, 3)}")
    return {"accuracy": accuracy}


@test
def test_knn(output, *args):
    """Assert KNN accuracy is at or above the minimum threshold."""
    acc = output["accuracy"]
    assert acc >= MIN_ACCURACY, (
        f"KNN accuracy {acc:.3f} is below threshold {MIN_ACCURACY}"
    )
