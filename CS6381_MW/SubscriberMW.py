###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually

class SubscriberMW():
    def __init__ (self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None # will be a ZMQ REQ socket to talk to Discovery service
        self.sub = None # will be a ZMQ sub socket for dissemination
        self.poller = None # used to wait on incoming replies
        self.addr = None # our advertised IP address
        self.port = None # port num
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop

    def configure (self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("SubscriberMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            # Next get the ZMQ context
            self.logger.debug ("SubscriberMW::configure - obtain ZMQ context")
            context = zmq.Context ()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug ("SubscriberMW::configure - obtain the poller")
            self.poller = zmq.Poller ()

            self.logger.debug ("SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket (zmq.REQ)
            self.sub = context.socket (zmq.SUB)

            self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register (self.req, zmq.POLLIN)
            self.poller.register (self.sub, zmq.POLLIN)

            self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = "tcp://" + args.discovery
            self.req.connect (connect_str)

            # self.logger.debug ("SubscriberMW::configure - bind to the pub socket")

            # connect_string = "tcp://*:" + str(self.port)
            # self.sub.connect (connect_string)

            self.logger.info ("SubscriberMW::configure completed")

        except Exception as e:
            raise e
    def event_loop (self, timeout=None):
        try:
            self.logger.info ("SubscriberMW::event_loop - run the event loop")

            while self.handle_events:  
                events = dict (self.poller.poll (timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation ()
                elif self.req in events:
                    timeout = self.handle_reply ()
                elif self.sub in events:
                    timeout = self.receive_from_pub ()
                else:
                    raise Exception ("Unknown event after poll")
            self.logger.info ("SubscriberMW::event_loop - out of the event loop")
        except Exception as e:
            raise e
    
    def handle_reply (self):
        try:
            self.logger.info ("SubscriberMW::handle_reply")
            bytesRcvd = self.req.recv ()
            disc_resp = discovery_pb2.DiscoveryResp ()
            disc_resp.ParseFromString (bytesRcvd)
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                timeout = self.upcall_obj.register_response (disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                timeout = self.upcall_obj.lookup_response (disc_resp.lookup_resp)
            else:
                raise ValueError ("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e
    
    def register (self, name, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info ("SubscriberMW::register")
            self.logger.debug ("SubscriberMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo () # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr
            reg_info.port = self.port 
            self.logger.debug ("SubscriberMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug ("SubscriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq ()  # allocate 
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER 
            register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
            register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug ("SubscriberMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("SubscriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            disc_req.register_req.CopyFrom (register_req)
            self.logger.debug ("SubscriberMW::register - done building the outer message")

            for item in topiclist:
                self.sub.setsockopt(zmq.SUBSCRIBE, bytes(item, "utf-8"))

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("SubscriberMW::register - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("SubscriberMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e
        

    def lookup_publisher(self,topiclist):
        #send topic list to discovery
        #look up like register
        try:
            self.logger.info ("SubscriberMW::lookup")
            self.logger.debug ("SubscriberMW::lookup_req - populate the LookupPubByTopicReq")
            lookup_req = discovery_pb2.LookupPubByTopicReq () # allocate
            lookup_req.topiclist[:] = topiclist
            self.logger.debug ("SubscriberMW::lookup - done populating nested LookupPubByTopicReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("SubscriberMW::lookup - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
            disc_req.lookup_req.CopyFrom (lookup_req)
            self.logger.debug ("SubscriberMW::lookup - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("SubscriberMW::lookup - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("SubscriberMW::lookup - sent lookup message and now now wait for reply")

        except Exception as e:
            raise e
    
    #single publisher
    def receive_from_pub (self):
        try:
            self.logger.info ("SubscriberMW::receive from publisher")
            bytesRcvd = self.sub.recv ()
            data=str(bytesRcvd, "utf-8")
            self.upcall_obj.data_receive(data)
            self.logger.debug ("SubscriberMW::receive complete")
            return 0
        except Exception as e:
            raise e
            
    def connect_pub (self, pubaddr):
        try:
            self.logger.info ("SubscriberMW::lookup - connect to publisher")
            self.logger.debug ("SubscriberMW::lookup - connect to the pub socket")

            connect_string = "tcp://" + str(pubaddr)
            self.sub.connect (connect_string)
 
            self.logger.debug ("SubscriberMW::connect complete")
        except Exception as e:
            raise e

    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj


    def disable_event_loop (self):
        ''' disable event loop '''
        self.handle_events = False