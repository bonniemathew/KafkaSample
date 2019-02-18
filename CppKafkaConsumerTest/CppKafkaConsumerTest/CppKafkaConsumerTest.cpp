// CppKafkaConsumerTest.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"
#include <iostream>
#include <cstdlib>

#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;

using cppkafka::Consumer;
using cppkafka::Configuration;
using cppkafka::Message;
using cppkafka::TopicPartitionList;

int main()
{
	// Build a configuration to be used on e.g. a consumer/producer
	Configuration config = {
		{ "metadata.broker.list", "trezikafka.centralindia.cloudapp.azure.com:9092" },
		{ "enable.auto.commit", false },
		{ "group.id", "test-group1" }
	};


	// Construct from some config we've defined somewhere
	Consumer consumer(config);

	// Subscribe to 2 topics
	consumer.subscribe({ "testtopic-bonnie"});

	// Now loop forever polling for messages
	while (true) {
		Message msg = consumer.poll();

		// Make sure we have a message before processing it
		if (!msg) {
			continue;
		}

		// Messages can contain error notifications rather than actual data
		if (msg.get_error()) {
			// librdkafka provides an error indicating we've reached the
			// end of a partition every time we do so. Make sure it's not one
			// of those cases, as it's not really an error
			if (!msg.is_eof()) {
				// Handle this somehow...
			}
			continue;
		}

		// We actually have a message. At this point you can check for the
		// message's key and payload via `Message::get_key` and
		// `Message::get_payload`

		string payload = msg.get_payload();

		DWORD msg_tick_time = std::strtoul(payload.c_str(), NULL, 0);
		DWORD delta = GetTickCount() - msg_tick_time;
		cout << "Message Key:" << msg.get_key() << "\t"<<"Message payload: "<<msg.get_payload()<< " lag = "<<delta<<endl;
	}
}
