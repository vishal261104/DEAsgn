try:
    from mage_ai.data_preparation.decorators import data_exporter
except ImportError:
    def identity_decorator(func=None, *args, **kwargs):
        if func is not None and callable(func):
            return func

        def decorator(f):
            return f

        return decorator

    data_exporter = identity_decorator

OUTPUT_PATH = "iris_predictions.csv"


@data_exporter
def export_data(df, *args, **kwargs):
    """Export the prediction results to a CSV file.

    Args:
        df (pd.DataFrame): Test rows with 'predicted' column from train_model.
    """
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} predictions to {OUTPUT_PATH}")
