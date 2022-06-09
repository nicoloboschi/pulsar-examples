# pulsar-examples

Usage:
```
./mvnw package
```

Local run on Pulsar docker image:

```
jar_path=<your_path>/pulsar-examples/java-functions/target/java-functions-1.0-SNAPSHOT.jar
docker cp ${jar_path} pulsar:/pulsar/java-functions-1.0-SNAPSHOT.jar

docker exec -it pulsar bin/pulsar-admin functions localrun \
  --tenant public \
  --namespace default \
  --name java_function \
  --classname com.nicoloboschi.javafunctions.ConcatAvroKeyValue \
  --jar /pulsar/java-functions-1.0-SNAPSHOT.jar \
  --inputs persistent://public/default/data-ks1.table1 \
  --output persistent://public/default/events-ks1.table2
```




