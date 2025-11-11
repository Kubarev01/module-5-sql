from opentelemetry import metrics

meter = metrics.get_meter_provider().get_meter(
    name="book-service",
    version="0.1.0",
)

book_created_counter = meter.create_counter(
    name="books_created_total",
    unit="1",
    description="Total number of created books",
)