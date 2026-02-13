# src/polydb/validation.py
"""
Model metadata validation and schema enforcement
"""
from typing import Type, Dict, Any, List, Optional
from dataclasses import dataclass
from .errors import InvalidModelMetadataError, ValidationError

@dataclass
class ValidationResult:
    valid: bool
    errors: List[str]
    warnings: List[str]


class ModelValidator:
    """Validates model metadata and schema"""
    
    REQUIRED_FIELDS = {'storage'}
    VALID_STORAGE_TYPES = {'sql', 'nosql'}
    SQL_REQUIRED_FIELDS = {'table'}
    NOSQL_OPTIONAL_FIELDS = {'collection', 'pk_field', 'rk_field'}
    
    @classmethod
    def validate_model(cls, model: Type) -> ValidationResult:
        """Comprehensive model validation"""
        errors = []
        warnings = []
        
        # Check __polydb__ exists
        if not hasattr(model, '__polydb__'):
            errors.append(f"Model {model.__name__} missing __polydb__ metadata")
            return ValidationResult(valid=False, errors=errors, warnings=warnings)
        
        meta = getattr(model, '__polydb__')
        
        # Check it's a dict
        if not isinstance(meta, dict):
            errors.append(f"__polydb__ must be a dict, got {type(meta)}")
            return ValidationResult(valid=False, errors=errors, warnings=warnings)
        
        # Check required fields
        for field in cls.REQUIRED_FIELDS:
            if field not in meta:
                errors.append(f"Missing required field: {field}")
        
        # Validate storage type
        storage = meta.get('storage')
        if storage and storage not in cls.VALID_STORAGE_TYPES:
            errors.append(f"Invalid storage type: {storage}. Must be one of {cls.VALID_STORAGE_TYPES}")
        
        # Storage-specific validation
        if storage == 'sql':
            for field in cls.SQL_REQUIRED_FIELDS:
                if field not in meta:
                    errors.append(f"SQL storage requires field: {field}")
        
        elif storage == 'nosql':
            # NoSQL has optional fields, warn if missing common ones
            if 'pk_field' not in meta:
                warnings.append("No pk_field specified, will default to 'id'")
            
            if not meta.get('collection') and not meta.get('table'):
                warnings.append("No collection/table name specified, will use model name")
        
        # Validate cache settings
        if meta.get('cache') and not isinstance(meta.get('cache_ttl'), (int, type(None))):
            errors.append("cache_ttl must be an integer or None")
        
        # Validate provider
        valid_providers = {'azure', 'aws', 'gcp', 'vercel', 'mongodb', 'postgresql'}
        provider = meta.get('provider')
        if provider and provider.lower() not in valid_providers:
            warnings.append(f"Unusual provider: {provider}")
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    @classmethod
    def validate_and_raise(cls, model: Type):
        """Validate and raise if invalid"""
        result = cls.validate_model(model)
        
        if not result.valid:
            raise InvalidModelMetadataError(
                f"Invalid model {model.__name__}: {', '.join(result.errors)}"
            )
        
        # Log warnings
        if result.warnings:
            import logging
            logger = logging.getLogger(__name__)
            for warning in result.warnings:
                logger.warning(f"{model.__name__}: {warning}")


class SchemaValidator:
    """Validates data against model schema"""
    
    @staticmethod
    def validate_data(model: Type, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against model requirements"""
        errors = []
        warnings = []
        
        meta = getattr(model, '__polydb__', {})
        
        # Check required keys exist
        pk_field = meta.get('pk_field', 'id')
        if pk_field not in data and not meta.get('auto_generate'):
            errors.append(f"Missing required field: {pk_field}")
        
        # Check field types if type hints available
        if hasattr(model, '__annotations__'):
            annotations = model.__annotations__
            for field_name, expected_type in annotations.items():
                if field_name in data:
                    actual_value = data[field_name]
                    if not isinstance(actual_value, expected_type):
                        warnings.append(
                            f"Field {field_name} expected {expected_type}, got {type(actual_value)}"
                        )
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )