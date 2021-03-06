// RdKafkaProducer.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"
#include <iostream>
#include "librdkafkacpp/rdkafkacpp.h"



int main()
{
	std::string brokers = "trezikafka.centralindia.cloudapp.azure.com:9092";
	std::string topic_str = "testtopic-bonnie";
	std::string errstr;
	bool run = true;
	int32_t partition = RdKafka::Topic::PARTITION_UA;

	/*
	 * Create configuration objects
	 */
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	/*
    * Set configuration properties
    */
	conf->set("metadata.broker.list", brokers, errstr);
	//tconf->set("metadata.broker.list", brokers, errstr);
	
	//RdKafka::Conf::ConfResult res;
	//res = tconf->set(name + strlen("topic."), val, errstr);


	/*
	* Create producer using accumulated global configuration.
	*/
	RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
	if (!producer) {
		std::cerr << "Failed to create producer: " << errstr << std::endl;
		exit(1);
	}

	std::cout << "% Created producer : " << producer->name() << std::endl;

	/*
	 * Read messages from stdin and produce to broker.
	 */
	for (std::string line; run && std::getline(std::cin, line);) {
		if (line.empty()) {
			producer->poll(0);
			continue;
		}
		RdKafka::Headers *headers = RdKafka::Headers::create();
		headers->add("my header", "header value");
		headers->add("other header", "yes");

		/*
		 * Produce message
		 */
		RdKafka::ErrorCode resp =
			producer->produce(topic_str, partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
				/* Value */
				const_cast<char *>(line.c_str()), line.size(),
				/* Key */
				NULL, 0,
				/* Timestamp (defaults to now) */
				0,
				/* Message headers, if any */
				headers,
				/* Per-message opaque value passed to
				 * delivery report */
				NULL);

		if (resp != RdKafka::ERR_NO_ERROR) {
			std::cerr << "% Produce failed: " << RdKafka::err2str(resp) << std::endl;
			delete headers; /* Headers are automatically deleted on produce()
							 * success. */
		}
		else {
			std::cerr << "% Produced message (" << line.size() << " bytes)" << std::endl;
		}
		producer->flush(1000);
		producer->poll(0);
	}


	while (run && producer->outq_len() > 0) {
		std::cerr << "Waiting for " << producer->outq_len() << std::endl;
		producer->poll(1000);
	}

	delete producer;
}

