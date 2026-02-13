# src/polydb/registry.py
from typing import Dict, Type


class ModelRegistry:
    """
    Lightweight model registry.

    Supports:
    - register(model)
    - get(model) → metadata
    - resolve(model_or_name) → model class
    """

    _models: Dict[Type, Dict] = {}

    @classmethod
    def register(cls, model):
        meta = getattr(model, "__polydb__", None)
        if not meta:
            raise ValueError(f"{model} missing __polydb__ config")

        cls._models[model] = meta

    # -------------------------
    # NEW: resolve()
    # -------------------------
    @classmethod
    def resolve(cls, model):
        """
        Resolve model class from:
        - class
        - class name string

        Returns actual class
        """
        # Already class
        if isinstance(model, type):
            if model not in cls._models:
                raise ValueError(f"Model not registered: {model.__name__}")
            return model

        # String name
        if isinstance(model, str):
            for m in cls._models:
                if m.__name__ == model:
                    return m

            raise ValueError(f"Model not registered: '{model}'")

        raise TypeError(f"Invalid model reference {model!r}. Must be class or class name.")

    @classmethod
    def get(cls, model):
        """Get model metadata"""
        from .types import ModelMeta

        model_cls = cls.resolve(model)
        raw_meta = cls._models[model_cls]

        # Convert to ModelMeta
        return ModelMeta(
            storage=raw_meta.get("storage", "sql"),
            table=raw_meta.get("table"),
            collection=raw_meta.get("collection"),
            pk_field=raw_meta.get("pk_field"),
            rk_field=raw_meta.get("rk_field"),
            provider=raw_meta.get("provider"),
            cache=raw_meta.get("cache", False),
            cache_ttl=raw_meta.get("cache_ttl"),
        )
