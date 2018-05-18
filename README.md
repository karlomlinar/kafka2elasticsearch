# KAFKA TO ELASTICSEARCH

(Some tests were written poorly and forgotten and also they might fail. This was fixed on branch fix_test)

### PERQUISITES
 - Installed docker
 - Downloaded Elasticsearch ([Download link](https://www.elastic.co/downloads/past-releases/elasticsearch-6-2-2))

##### Elasticsearch
To run Elasticsearch, unzip downloaded file, go to *elasticsearch-6.2.2/bin* and start *elasticsearch.bat*
After Elasticsearch is started and running, cd to */elasticsearch-setup* and run [es-setup](elasticsearch-setup/es-setup.sh) (run ```./es-setup.sh``` in your favorite bash)

##### Kafka
In bash run ```docker pull landoop/fast-data-dev``` to get the latest version of Landoop Kafka environment.

After it has been downloaded, run the image with command: ```docker run --rm -it -p 2181:2181 -p 9092:9092 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 3030:3030 -e ADV_HOST=127.0.0.1 landoop/fast-data-dev```

You still need to create two topics, to do that, you have to open another bash and enter command: ```docker run --rm -it --net=host landoop/fast-data-dev bash```

Now, create the topics by executing: 
- ```kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 6 --topic message_log```
- ```kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 6 --topic message_price```

### INSTRUCTIONS
To build project, execute ```./gradlew build``` at the root this project

To run this project, cd to */k2e-main* and execute ```./gradlew bootrun```

To send a couple of messages to test the app while it's running, cd to */k2e-message-producer* and execute ```./gradlew bootrun```

```message_log``` message value example:

```
{  
   "id":"E2ETestId",
   "networkId":4,
   "status":"COMPLETED",
   "date":1526630605,
   "final":false
}
```

```message_price``` message value example:

```
{  
   "id":"abc",
   "accountId":1,
   "messageLogId":"E2ETestId",
   "price":18,
   "final":false
}
```

Message key in topic ```message_log``` is ```id```, in topic ```message_price``` the key is ```messageLogId```
