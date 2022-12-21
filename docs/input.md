
Input
-----

As with the Python implementation of carbon-relay, metrics can be pushed to carbon-relay-ng via TCP
(plain text or pickle) or by using an AMQP broker such as RabbitMQ. To send metrics via AMQP, create
a topic exchange (named "metrics" in the example carbon-relay-ng.ini) and publish messages to it in
the usual metric format: `<metric path> <metric value> <metric timestamp>`. An exclusive, ephemeral
queue will automatically be created and bound to the exchange, which carbon-relay-ng will consume from.


## Listen text input

setting              | mandatory | values   | default | description
---------------------|-----------|----------|---------|------------
format               | N         | string   | plain   |Only plain is implemented
strict               | N         | bool     | false   |unused
max_length           | N         | Int      | 0       |drop metrics with name above value
workers              | N         | Int      | 0       |number of workers
listen_addr          | Y         | String   | 1000    |listen_address
read_timeout         | N         | int (ms) | 10k     |read timeout
