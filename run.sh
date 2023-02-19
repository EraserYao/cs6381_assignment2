python3 DiscoveryAppln.py -P 2 -S 2
python3 PublisherAppln.py -T 5
python3 PublisherAppln.py -T 5 -n pub2 -p 5578
python3 SubscriberAppln.py
python3 SubscriberAppln.py -n sub2 -p 5678
python3 BrokerAppln.py
