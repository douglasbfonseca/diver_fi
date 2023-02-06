from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Registra as pipelines do projeto.

    Returns:
        Um mapa de 'Pipeline' objects.
    """
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines
