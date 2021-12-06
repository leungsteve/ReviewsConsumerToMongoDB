# ReviewsConsumerToMongoDB

This application performs the following:
- consumes Kafka messages from the reviews topic
- for each message:
-   GETs a sentiment from the sentiment service
-   Appends the sentiment to the message
-   Inserts the document in MongoDB, in the O11y DB and the O11yCollection collection

Kafka headers are populated by the producer with trace details. For example:
```
traceparent:00-c019109a085e7485023061a077aa719b-81bb5dce0a2b2363-01	{"review_id":"aJGiadFyQq3ql1UBr6D37w","user_id":"usBt8QwqaL94Hx840KoLag","business_id":"Zdvnu_d4s-Vssv0grvblkA","stars":1.0,"useful":1,"funny":0,"cool":0,"text":"ROOFTOP POOL: what a bummer. I went last year and had two awful experiences but figured maybe that was bc it was it's first summer open. When I went back this year it was equally bad. We waited about 30 min for waitstaff...they never came to our table...so I went to the bar and stood there for about 8 minutes when the bartender chatted w her friend (and I was the only customer at the bar!). Drinks are small and overpriced. Food was awful (hummus plate). The pool might as well not be there : it's sort of inside, filled w little kids, and looks dirty\/dated. The cabana area is lovely if you can deal w $$ entrance fee, the risk you might not even find a place to sit, rude poor service, bad food\/drinks, and no bathrooms (well, one, but you'll wait about an hour to get in).","date":"2014-08-26 02:27:05"}
```
With this example message:
- the trace ID is: c019109a085e7485023061a077aa719b
- the span  ID is: 81bb5dce0a2b2363

Once artifacts are built, the application is started with the following command:

```
export OTEL_SERVICE_NAME='reviews-consumer'
export OTEL_RESOURCE_ATTRIBUTES='deployment.environment=lab'
export OTEL_EXPORTER_OTLP_ENDPOINT='http://myotel:4317'
java -javaagent:./splunk-otel-javaagent.jar -jar ReviewsConsumerToMongoDB.jar
```
