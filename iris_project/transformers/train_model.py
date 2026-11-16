import joblib
from sklearn.tree import DecisionTreeClassifier

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

# The 4 measurement columns the model uses to predict the species
FEATURES = [
    "sepal length (cm)",
    "sepal width (cm)",
    "petal length (cm)",
    "petal width (cm)",
]

# Minimum acceptable test accuracy threshold
MIN_ACCURACY = 0.8


@transformer
def train(df, *args, **kwargs):
    """Train a DecisionTreeClassifier on the train split and return predictions.

    Args:
        df (pd.DataFrame): Combined train/test DataFrame with a 'split' column.

    Returns:
        pd.DataFrame: Test rows with an added 'predicted' column.
    """
    max_depth = int(kwargs.get("max_depth", 3))

    train_df = df[df["split"] == "train"]
    test_df = df[df["split"] == "test"].copy()

    model = DecisionTreeClassifier(max_depth=max_depth, random_state=42)
    model.fit(train_df[FEATURES], train_df["target"])

    test_df["predicted"] = model.predict(test_df[FEATURES])
    accuracy = (test_df["predicted"] == test_df["target"]).mean()
    print("max_depth =", max_depth, "| Test accuracy =", round(accuracy, 3))

    joblib.dump(model, "iris_model.pkl")
    return test_df


@test
def test_accuracy(output, *args):
    """Assert that test accuracy meets the minimum threshold."""
    accuracy = (output["predicted"] == output["target"]).mean()
    assert accuracy >= MIN_ACCURACY, (
        f"Model accuracy {accuracy:.3f} is below threshold {MIN_ACCURACY}"
    )
