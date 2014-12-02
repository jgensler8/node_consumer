var commander = require('commander');

commander
  .version('0.0.1')
  .option('-id, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-host [value]', 'iKafka idomain to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .option('-zh, --zookeeper-host [value]', 'Zookeeper host to cionnect to.')
  .option('-zp, --zookeeper-port [n]', 'Zookeeper port to cionnect to.', parseInt)
  .option('-hbh, --hbase-host [value]', 'HBase host to cionnect to.')
  .option('-hbp, --hbase-port [n]', 'HBase port to cionnect to.', parseInt)
  .option('-rh, --redis-host [value]', 'Redis host to cionnect to.')
  .option('-rp, --redis-port [n]', 'Redis port to cionnect to.', parseInt)
  .parse(process.argv);

if(commander.id === undefined) commander.id = "node-consumer-" + ((Math.random() + 10000000) % 10000000);
if(commander.kafkaHost === undefined) commander.kafkaHost = 'kafka';
if(commander.kafkaPort === undefined) commander.kafkaPort = 9092;
if(commander.zookeeperHost === undefined) commander.zookeeperHost = 'zookeeper';
if(commander.zookeeperHost === undefined) commander.zookeeperPort = 2181;
if(commander.hbaseHost === undefined) commander.hbaseHost = 'hbase';
if(commander.hbasePort === undefined) commander.hbasePort = 60000;
if(commander.redisHost === undefined) commander.redisHost = 'redis';
if(commander.redisPort === undefined) commander.redisPort = 6379;

var hbase = require('hbase')({ host: commander.hbaseHost, port: commander.hbasePort});

var redis = require('redis'),
  client = redis.createClient( commander.redisPort, commander.redisHost);

console.log(commander.kafkaHost, commander.kafkaPort, commander.id);

// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: commander.kafkaHost, port: commander.kafkaPort}],
  clientId: commander.id,
  maxBytes: 2000000
});

// tearup (start) the client
kafkaesque.tearUp(function() {
  console.log("Kafka connected...");
  
  // poll the testing topic, kafakesque will determine the lead broker for this
  // partition / topic pairing and will emit messages as they become available
  // kafakesque will maintain the read position on the topic based on calls to
  // commit()
  kafkaesque.poll({topic: 'fridge', partition: 0},
                  function(err, kafka) {
    // handle each message
    kafka.on('message', function(index, message, commit) {
      //add to redis
      client.hset('fridges', JSON.parse(message.value).id, message.value);
      //add to hbase

      //print to console
      //console.log(JSON.parse(message.value).id, message.value);
      
      // once a message has been successfull handled, call commit to advance this
      // consumers position in the topic / parition
      commit();
    });
    // report errors
    kafka.on('error', function(error) {
      console.log(JSON.stringify(error));
    });
  });
});
