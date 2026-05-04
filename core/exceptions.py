class PipelineException(Exception):
    pass

class ConfigurationError(PipelineException):
    pass

class ExtractionError(PipelineException):
    pass

class ValidationError(PipelineException):
    pass

class LoadError(PipelineException):
    pass

class WatermarkError(PipelineException):
    pass

class AuditError(PipelineException):
    pass
