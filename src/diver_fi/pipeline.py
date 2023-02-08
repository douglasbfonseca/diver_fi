from kedro.pipeline import Pipeline, node, pipeline
from .nodes import extract, transform, load


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract,
                inputs=["parameters"],
                outputs="df_all",
                name="Extract",
            ),
            node(
                func=transform,
                inputs="df_all",
                outputs=["df_cnpj" , "df_percentual", "df_one_hot"],
                name="Transform",
            ),
            node(
                func=load,
                inputs=["df_cnpj", "df_percentual", "df_one_hot", "parameters"],
                outputs=None,
                name="load",
            ),
        ]
    )