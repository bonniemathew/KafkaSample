// cppkafkaproducertest.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"
#include <iostream>
#include <cppkafka/configuration.h>
#include <cppkafka/producer.h>

using namespace cppkafka;
using namespace std;

int main()
{
	// Create the config
	Configuration config = {
		{ "metadata.broker.list", "trezikafka.centralindia.cloudapp.azure.com:9092" }
	};

	// Create the producer
	Producer producer(config);
	int count = 0;

	while (true)
	{
		// Produce a message!
		string msg_key = string("msg_") + string(to_string(count++));
		string message = to_string(GetTickCount());
		producer.produce(MessageBuilder("testtopic-bonnie").key(msg_key).partition(0).payload(message));
		cout << "Producer: " << msg_key << ": " << message << endl;
		producer.flush();
	}
}

