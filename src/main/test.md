# docker exec -it kafka1 /bin/bash
cd  /usr/bin

# producer
/usr/bin//kafka-console-producer --bootstrap-server kafka1:19091,kafka2:19092,kafka3:19093 --topic test-topic


# consumer
/usr/bin//kafka-console-consumer --bootstrap-server kafka1:19091,kafka2:19092,kafka3:19093 --topic test-topic --from-beginning



