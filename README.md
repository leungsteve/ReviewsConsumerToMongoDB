# ReviewsConsumerToMongoDB

Once artifacts are built, the application is started with the following command:

```
export OTEL_SERVICE_NAME='reviews-consumer'
export OTEL_RESOURCE_ATTRIBUTES='deployment.environment=lab'
export OTEL_EXPORTER_OTLP_ENDPOINT='http://myotel:4317'
java -javaagent:./splunk-otel-javaagent.jar -jar ReviewsConsumerToMongoDB.jar
```
