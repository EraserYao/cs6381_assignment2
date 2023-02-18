###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# BrokerMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually

class BrokerMW():
    def __init__ (self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None # will be a ZMQ REP socket
        self.pub = None
        self.sub = None
        self.poller = None # used to wait on incoming replies
        self.addr = None # our advertised IP address
        self.port = None # port num
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop

    def configure (self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("BrokerMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            # Next get the ZMQ context
            self.logger.debug ("BrokerMW::configure - obtain ZMQ context")
            context = zmq.Context ()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug ("BrokerMW::configure - obtain the poller")
            self.poller = zmq.Poller ()

            self.logger.debug ("BrokerMW::configure - obtain REP sockets")
            self.req = context.socket (zmq.REQ)
            self.pub = context.socket (zmq.PUB)
            self.sub = context.socket (zmq.SUB)

            self.logger.debug ("BrokerMW::configure - register the REQ socket for incoming replies")
            self.poller.register (self.req, zmq.POLLIN)

            self.logger.debug ("BrokerMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = "tcp://" + args.discovery
            self.req.connect (connect_str)

            self.logger.debug ("BrokerMW::configure - bind to the pub socket")
            
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind (bind_string)
    
            self.logger.info ("BrokerMW::configure completed")

        except Exception as e:
            raise e
        
    def event_loop (self, timeout=None):
        try:
            self.logger.info ("BrokerMW::event_loop - run the event loop")

            while self.handle_events:  
                events = dict (self.poller.poll (timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation ()
                elif self.rep in events:
                    timeout = self.handle_request ()
                else:
                    raise Exception ("Unknown event after poll")
            self.logger.info ("BrokerMW::event_loop - out of the event loop")
        except Exception as e:
            raise e
        
    def handle_reply (self):
        try:
            self.logger.info ("BrokerMW::handle_reply")
            bytesRcvd = self.req.recv ()
            disc_resp = discovery_pb2.DiscoveryResp ()
            disc_resp.ParseFromString (bytesRcvd)
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                timeout = self.upcall_obj.register_response (disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                timeout = self.upcall_obj.lookup_response (disc_resp.lookup_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                timeout = self.upcall_obj.isready_response (disc_resp.isready_resp)
            else:
                raise ValueError ("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e
        
    
    def register (self, name):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info ("BrokerMW::register")
            self.logger.debug ("BrokerMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo () # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr
            reg_info.port = self.port 
            self.logger.debug ("BrokerMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug ("BrokerMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq ()  # allocate 
            register_req.role = discovery_pb2.ROLE_BOTH
            register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
            self.logger.debug ("BrokerMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("BrokerMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            disc_req.register_req.CopyFrom (register_req)
            self.logger.debug ("BrokerMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("BrokerMW::register - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e
        
    def is_ready (self):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info ("BrokerMW::is_ready")

            # first build a IsReady message
            self.logger.debug ("BrokerMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq ()  # allocate 
            # actually, there is nothing inside that msg declaration.
            self.logger.debug ("BrokerMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug ("BrokerMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_req.CopyFrom (isready_req)
            self.logger.debug ("BrokerMW::is_ready - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("BrokerMW::is_ready - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::is_ready - request sent and now wait for reply")

        except Exception as e:
            raise e

    def disseminate (self, id, topic, data):
        try:
          self.logger.debug ("BrokerMW::disseminate")

          # Now use the protobuf logic to encode the info and send it.  But for now
          # we are simply sending the string to make sure dissemination is working.
          send_str = topic + ":" + data
          self.logger.debug ("BrokerMW::disseminate - {}".format (send_str))

          # send the info as bytes. See how we are providing an encoding of utf-8
          self.pub.send (bytes(send_str, "utf-8"))

          self.logger.debug ("BrokerMW::disseminate complete")
        except Exception as e:
          raise e
        
    def lookup_publisher(self,topiclist):
        #send topic list to discovery
        #look up like register
        try:
            self.logger.info ("BrokerMW::lookup")
            self.logger.debug ("BrokerMW::lookup_req - populate the LookupPubByTopicReq")
            lookup_req = discovery_pb2.LookupPubByTopicReq () # allocate
            lookup_req.topiclist[:] = topiclist
            self.logger.debug ("BrokerMW::lookup - done populating nested LookupPubByTopicReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("BrokerMW::lookup - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
            disc_req.lookup_req.CopyFrom (lookup_req)
            self.logger.debug ("BrokerMW::lookup - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("BrokerMW::lookup - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::lookup - sent lookup message and now now wait for reply")

        except Exception as e:
            raise e
    
    def receive_data (self, id, pubaddr):
        try:
            self.logger.debug ("BrokerMW::receive")
            self.logger.debug ("BrokerMW::receive - connect to the pub socket")

            connect_string = "tcp://" + str(pubaddr)
            self.sub.connect (connect_string)
      
            bytesRcvd = self.req.recv ()
            data=str(bytesRcvd)
            self.logger.debug ("BrokerMW::receive complete")
            return str(data)
        except Exception as e:
            raise e