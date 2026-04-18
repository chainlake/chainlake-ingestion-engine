# load YAML + env

import yaml
from .schema import PipelineConfig

def load_pipeline_config(path: str) -> PipelineConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    return PipelineConfig(**raw)