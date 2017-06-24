Kafka JDBC Connector Samples
============================

Sample projects using Kafka JDBC Connector.

Install and Run
---------------

Add following entry in `/etc/hosts` file

```
127.0.0.1	kafka
```

Run following commands form the terminal

```sh
sbt docker
docker-compose up -d
sbt functional/test
```

License
-------

Kafka JDBC Connector is Open Source and available under the [MIT License](https://github.com/agoda-com/kafka-jdbc-connector-samples/blob/master/LICENSE).
