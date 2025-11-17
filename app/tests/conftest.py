import os
os.environ.setdefault("OTEL_SDK_DISABLED", "true")
os.environ.setdefault("OTEL_TRACES_EXPORTER", "none")
os.environ.setdefault("ZIPKIN_HOST", "")
os.environ.setdefault("OTEL_EXPORTER_ZIPKIN_ENDPOINT", "")