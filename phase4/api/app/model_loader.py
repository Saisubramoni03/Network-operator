import os
import joblib

MODEL_ARTIFACT_PATH = os.environ.get(
    "RISK_MODEL_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "phase6", "ml", "model_artifacts", "risk_model.joblib")
)

_artifact = None


def load_model():
    """Loads the trained model artifact once and caches it in memory.
    Raises a clear, actionable error if it's missing, so the app fails
    loudly at startup instead of silently serving a broken stub."""
    global _artifact
    if _artifact is not None:
        return _artifact

    if not os.path.exists(MODEL_ARTIFACT_PATH):
        raise FileNotFoundError(
            f"Risk model artifact not found at {MODEL_ARTIFACT_PATH}. "
            f"Run phase6/ml/train_risk_model.py first to train and persist it."
        )

    _artifact = joblib.load(MODEL_ARTIFACT_PATH)
    return _artifact