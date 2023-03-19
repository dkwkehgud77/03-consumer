# docker exec -it kafka1 /bin/bash
cd  /usr/bin

# producer
./kafka-console-producer --bootstrap-server kafka1:19091,kafka2:19092,kafka3:19093 --topic test-topic


# consumer
./kafka-console-consumer --bootstrap-server kafka1:19091,kafka2:19092,kafka3:19093 --topic test-topic --from-beginning



