

/usr/bin/connect-standalone /etc/schema-registry/connect-avro-standalone.properties /etc/kafka-connect-jdbc/sink-postgresql.properties


# /etc/kafka-connect-jdbc/sink-postgresql.properties
connection.url=jdbc:psql://ec2-44-232-35-168.us-west-2.compute.amazonaws.com:5432/gistaxidata
connection.user=postmaster
connection.password=JD5WGVMQ
auto.create=true
