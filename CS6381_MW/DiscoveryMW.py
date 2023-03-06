###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually

class DiscoveryMW():
    def __init__ (self, logger):
        self.logger = logger  # internal logger for print statements
        self.rep = None # will be a ZMQ REP socket
        self.req = [] # will be a ZMQ REQ socket
        self.poller = None # used to wait on incoming replies
        self.addr = None # our advertised IP address
        self.port = None # port num
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop
        self.m=48  #hash_bit

    def configure (self, addr, port):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = port
            self.addr = addr

            # Next get the ZMQ context
            self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
            context = zmq.Context ()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug ("DiscoveryMW::configure - obtain the poller")
            self.poller = zmq.Poller ()

            self.logger.debug ("DiscoveryMW::configure - obtain REP sockets")
            self.rep = context.socket (zmq.REP)
            for i in range(self.m):
                self.req.append(context.socket (zmq.REQ))

            self.logger.debug ("DiscoveryMW::configure - register the REQ socket for incoming replies")
            self.poller.register (self.rep, zmq.POLLIN)
            for i in range(self.m):
                self.poller.register (self.req[i], zmq.POLLIN)

            self.logger.debug ("DiscoveryMW::configure - bind the port")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            bind_str = "tcp://*:" + str(self.port)
            self.rep.bind (bind_str)
            self.logger.info ("DiscoveryMW::configure completed")

        except Exception as e:
            raise e
        
    def connect_disc (self, index, discaddr):
        try:
            self.logger.info ("DiscoveryMW::connect_disc - connect to publisher")
            self.logger.debug ("DiscoveryMW::connect_disc - connect to the pub socket")

            connect_string = "tcp://" + str(discaddr)
            self.req[index].connect (connect_string)
 
            self.logger.debug ("DiscoveryMW::connect_disc complete")
        except Exception as e:
            raise e

    def event_loop (self, timeout=None):
        try:
            self.logger.info ("DiscoveryMW::event_loop - run the event loop")

            while self.handle_events: 
                events = dict (self.poller.poll (timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation ()
                elif self.rep in events:
                    timeout = self.handle_request ()
                for i in range (len (self.req)):
                    if self.req[i] in events:
                        timeout = self.handle_reply (i)
                else:
                    raise Exception ("Unknown event after poll")
            self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
        except Exception as e:
            raise e
    
    def handle_request (self):
        try:
            self.logger.info ("DiscoveryMW::handle_request")
            bytesRcvd = self.rep.recv ()
            disc_req = discovery_pb2.DiscoveryReq ()
            disc_req.ParseFromString (bytesRcvd)
            if(disc_req.dht_type==discovery_pb2.TYPE_SUCCESSOR):
                if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                    timeout = self.upcall_obj.register_request (disc_req.dht_req.register_req)
                elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                    timeout = self.upcall_obj.isready_request (disc_req.dht_req.isready_req)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                    timeout = self.upcall_obj.lookup_request (disc_req.dht_req.lookup_req)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                    timeout = self.upcall_obj.lookall_request (disc_req.dht_req.lookall_req)
            elif(disc_req.dht_type==discovery_pb2.TYPE_PRENODE):
                timeout = self.upcall_obj.chord_algurithm (disc_req)
            elif(disc_req.dht_type==discovery_pb2.TYPE_INITIAL):
                if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                    timeout = self.upcall_obj.register_request_encode (disc_req.register_req)
                elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                    timeout = self.upcall_obj.isready_request_encode (disc_req.isready_req)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                    timeout = self.upcall_obj.lookup_request_encode (disc_req.lookup_req)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                    timeout = self.upcall_obj.lookall_request_encode (disc_req.lookall_req)
            else:
                raise ValueError ("Unrecognized request message")
            return timeout
        except Exception as e:
            raise e

    def handle_reply (self, index, DHT_type):
        try:
            self.logger.info ("DiscoveryMW::handle_reply")
            bytesRcvd = self.req[index].recv ()

            # relay response here
            self.logger.debug ("DiscoveryMW::transmit DHT data")
            self.rep.send(bytesRcvd)
            return None
        except Exception as e:
            raise e

    def relay_chord_req(self,index,DHT_type,disc_req):
        self.logger.info ("DiscoveryMW::relay DHT request")

        self.logger.debug ("DiscoveryMW::relay DHT request - build the outer DiscoveryReq message")
        relay_disc_req = discovery_pb2.DiscoveryReq ()  # allocate
        relay_disc_req.msg_type=disc_req.msg_type
        relay_disc_req.dht_type=DHT_type
        relay_disc_req.dht_req.CopyFrom (disc_req.dht_req)
        self.logger.debug ("DiscoveryMW::send DHT register request - done building the outer message")

        buf2send = relay_disc_req.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW::end DHT register request - send stringified buffer")
        self.req[index].send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info ("DiscoveryMW::end DHT register request - sent response message")

    def send_chord_register_req(self,index,DHT_type,hash_value,register_req):
        self.logger.info ("DiscoveryMW::send DHT register request")

        self.logger.debug ("DiscoveryMW::send DHT register request - build the outer DHTReq message")
        dht_req= discovery_pb2.DHTReq () 
        dht_req.key=hash_value
        dht_req.register_req.CopyFrom (register_req)
        self.logger.debug ("DiscoveryMW::send DHT register request - done building the DHTReq message")

        self.logger.debug ("DiscoveryMW::send DHT register request - build the outer DiscoveryReq message")
        disc_req = discovery_pb2.DiscoveryReq ()  # allocate
        disc_req.msg_type=discovery_pb2.TYPE_REGISTER
        disc_req.dht_type=DHT_type
        disc_req.dht_req.CopyFrom (dht_req)
        self.logger.debug ("DiscoveryMW::send DHT register request - done building the outer message")

        buf2send = disc_req.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW::end DHT register request - send stringified buffer")
        self.req[index].send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info ("DiscoveryMW::end DHT register request - sent response message")

    def send_register_resp(self,status,reason):
        self.logger.info ("DiscoveryMW::send register response")
        register_resp=discovery_pb2.RegisterResp ()
        register_resp.status=status
        if reason is not None:
            register_resp.reason=reason

        # Finally, build the outer layer DiscoveryResp Message
        self.logger.debug ("DiscoveryMW::register response - build the outer DiscoveryResp message")
        disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
        disc_resp.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_resp.register_resp.CopyFrom (register_resp)
        self.logger.debug ("DiscoveryMW::register response - done building the outer message")
    
        buf2send = disc_resp.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW::register response - send stringified buffer")
        self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info ("DiscoveryMW::register response - sent response message")

    def send_isready_resp(self,is_ready):
        self.logger.info ("DiscoveryMW::send isready response")
        isready_resp=discovery_pb2.IsReadyResp ()
        isready_resp.status=is_ready
        

        # Finally, build the outer layer DiscoveryResp Message
        self.logger.debug ("DiscoveryMW::isready response - build the outer DiscoveryResp message")
        disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
        disc_resp.msg_type = discovery_pb2.TYPE_ISREADY  # set message type
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_resp.isready_resp.CopyFrom (isready_resp)
        self.logger.debug ("DiscoveryMW::isready response - done building the outer message")
    
        buf2send = disc_resp.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW::isready response - send stringified buffer")
        self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info ("DiscoveryMW::isready response - sent response message")

    def send_lookup_resp(self,publisherInfos):
        self.logger.info ("DiscoveryMW::send lookup response")
        lookup_resp=discovery_pb2.LookupPubByTopicResp ()
        lookup_resp.status=discovery_pb2.STATUS_SUCCESS
        
        for publisherInfo in publisherInfos:
            newPublisherInfo=discovery_pb2.RegistrantInfo()
            newPublisherInfo.id=publisherInfo.id
            newPublisherInfo.addr=publisherInfo.addr
            newPublisherInfo.port=publisherInfo.port
            lookup_resp.publisherInfos.append(newPublisherInfo)

        # Finally, build the outer layer DiscoveryResp Message
        self.logger.debug ("DiscoveryMW::lookup response - build the outer DiscoveryResp message")
        disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
        disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_resp.lookup_resp.CopyFrom (lookup_resp)
        self.logger.debug ("DiscoveryMW::lookup response - done building the outer message")
    
        buf2send = disc_resp.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW::lookup response - send stringified buffer")
        self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info ("DiscoveryMW::lookup response - sent response message")

    def send_lookall_resp(self,publisherInfos):
        self.logger.info ("DiscoveryMW::send lookall response")
        lookall_resp=discovery_pb2.LookupAllPubResp ()
        lookall_resp.status=discovery_pb2.STATUS_SUCCESS
        
        for publisherInfo in publisherInfos:
            newPublisherInfo=discovery_pb2.RegistrantInfo()
            newPublisherInfo.id=publisherInfo.id
            newPublisherInfo.addr=publisherInfo.addr
            newPublisherInfo.port=publisherInfo.port
            lookall_resp.publisherInfos.append(newPublisherInfo)

        # Finally, build the outer layer DiscoveryResp Message
        self.logger.debug ("DiscoveryMW::lookall response - build the outer DiscoveryResp message")
        disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
        disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS  # set message type
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_resp.lookall_resp.CopyFrom (lookall_resp)
        self.logger.debug ("DiscoveryMW::lookall response - done building the outer message")
    
        buf2send = disc_resp.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW::lookall response - send stringified buffer")
        self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info ("DiscoveryMW::lookall response - sent response message")

    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj