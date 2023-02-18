###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

class DiscoveryAppln():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        PENDING=2,
        READY=3

    def __init__(self,logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name=None
        self.pubnum=0
        self.subnum=0
        self.cur_pubnum=0
        self.cur_subnum=0
        self.mw_obj = None # handle to the underlying Middleware object
        self.is_ready=False
        self.logger = logger  # internal logger for print statements
        #temp data
        self.pub_data={}
        self.sub_data={}
        self.broker={}
        self.dissemination=None

    def configure(self,args):
        try:
            self.logger.info ("DiscoveryAppln::configure")
            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE
            # initialize our variables
            self.name=args.name
            self.pubnum=args.pubnum
            self.subnum=args.subnum

            # Now, get the configuration object
            self.logger.debug ("DiscoveryAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug ("DiscoveryAppln::configure - initialize the middleware object")
            self.mw_obj = DiscoveryMW (self.logger)
            self.mw_obj.configure (args) # pass remainder of the args to the m/w object
            
            self.logger.info ("DiscoveryAppln::configure - configuration complete")
      
        except Exception as e:
            raise e
        

    def driver (self):
        ''' Driver program '''

        try:
            self.logger.info ("DiscoveryAppln::driver")

            self.dump ()

            self.logger.debug ("DiscoveryAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle (self)

            self.state = self.State.PENDING

            self.mw_obj.event_loop (timeout=0)  # start the event loop

            self.logger.info ("DiscoveryAppln::driver completed")

        except Exception as e:
            raise e
        
    def invoke_operation (self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info ("DiscoveryAppln::invoke_operation")

            if (self.state == self.State.PENDING):
                # send a register msg to discovery service
                self.logger.debug ("DiscoveryAppln::invoke_operation - waiting for pub and sub to registration")
                if self.cur_pubnum==self.pubnum and self.cur_subnum==self.subnum:
                    if self.dissemination == "Broker":
                        if len(self.broker)>0:
                            self.state = self.State.READY
                            self.is_ready=True
                        else:
                            self.is_ready=False
                    else:
                        self.state = self.State.READY
                        self.is_ready=True
                return None
            
            elif (self.state == self.State.READY):
                self.logger.debug ("DiscoveryAppln::invoke_operation - handing")
                self.is_ready=True
                return None
            
            else:
                raise ValueError ("Undefined state of the appln object")

        except Exception as e:
            raise e
        
    def register_request(self,reg_req):
        try:
            self.logger.info ("DiscoveryAppln::register")
            status=discovery_pb2.STATUS_UNKNOWN
            reason=None
            reg_info = discovery_pb2.RegistrantInfo ()
            reg_info.CopyFrom(reg_req.info)
            if reg_req.role==discovery_pb2.ROLE_PUBLISHER:
                pub_name=reg_info.id
                if pub_name in self.pub_data.keys():
                    status=discovery_pb2.STATUS_FAILURE
                    reason='Name has already exits!'
                else:
                    self.cur_pubnum+=1
                    status=discovery_pb2.STATUS_SUCCESS
                    self.pub_data[pub_name]={}
                    self.pub_data[pub_name]['addr']=reg_info.addr
                    self.pub_data[pub_name]['port']=reg_info.port
                    self.pub_data[pub_name]['topiclist']=reg_req.topiclist[:]

            elif reg_req.role==discovery_pb2.ROLE_SUBSCRIBER:
                sub_name=reg_info.id
                if sub_name in self.sub_data.keys():
                    status=discovery_pb2.STATUS_FAILURE
                    reason='Name has already exits!'
                else:
                    self.cur_subnum+=1
                    status=discovery_pb2.STATUS_SUCCESS
                    self.sub_data[sub_name]={}
                    self.sub_data[sub_name]['topiclist']=reg_req.topiclist[:]

            elif reg_req.role==discovery_pb2.ROLE_BOTH:
                broker_name=reg_info.id
                if len(self.broker)>0:
                    status=discovery_pb2.STATUS_FAILURE
                    reason='Broker has already exits!'
                else:
                    self.broker['name']=broker_name
                    self.broker['addr']=reg_info.addr
                    self.broker['port']=reg_info.port
                    self.broker['topiclist']=reg_req.topiclist[:]
            else:
                raise ValueError ("Unknown type of request")
            
            self.mw_obj.send_register_resp(status,reason)
            # return a timeout of zero so that the event loop in its next iteration will immediately make
            # an upcall to us
            return 0
            
        except Exception as e:
            raise e

    
    def isready_request(self,isready_req):
        try:
            self.logger.info ("DiscoveryAppln::publisher is ready")
            self.mw_obj.send_isready_resp(self.is_ready)
            # return a timeout of zero so that the event loop in its next iteration will immediately make
            # an upcall to us
            return 0
            
        except Exception as e:
            raise e

    def lookup_request(self,lookup_req):
        try:
            self.logger.info ("DiscoveryAppln::subscriber lookup")
            
            sub_topiclist=lookup_req.topiclist[:]
            #get the topic
            if self.dissemination == "Broker":
                brokerInfo=discovery_pb2.RegistrantInfo()
                brokerInfo.id=self.broker['name']
                brokerInfo.addr=self.broker['addr']
                brokerInfo.port=self.broker['port']
                self.mw_obj.send_lookup_resp(brokerInfo)    
            else:
                publisherInfos=[]
                for pubname, publisher in self.pub_data.items():
                    if list(set(publisher['topiclist'])&set(sub_topiclist)):
                        publisherInfo=discovery_pb2.RegistrantInfo()
                        publisherInfo.id=pubname
                        publisherInfo.addr=publisher['addr']
                        publisherInfo.port=publisher['port']
                        publisherInfos.append(publisherInfo)

                self.mw_obj.send_lookup_resp(publisherInfos)
            # return a timeout of zero so that the event loop in its next iteration will immediately make
            # an upcall to us
            return 0
            
        except Exception as e:
            raise e

    def lookall_request(self,lookall_req):
        try:
            self.logger.info ("DiscoveryAppln::broker lookall")
            
            #get the topic
            if self.dissemination == "Broker":
                publisherInfos=[]
                for pubname, publisher in self.pub_data.items():
                    publisherInfo=discovery_pb2.RegistrantInfo()
                    publisherInfo.id=pubname
                    publisherInfo.addr=publisher['addr']
                    publisherInfo.port=publisher['port']
                    publisherInfos.append(publisherInfo)

                self.mw_obj.send_lookall_resp(publisherInfos)
            else:
                raise ValueError ("Not broker, not allowed")
            # return a timeout of zero so that the event loop in its next iteration will immediately make
            # an upcall to us
            return 0
            
        except Exception as e:
            raise e
        
    def dump (self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("DiscoveryAppln::dump")
            self.logger.info ("THIS IS DISCOVERY :D")
            self.logger.info ("------------------------------")
            self.logger.info ("     name: {}".format (self.name))
            self.logger.info ("     Num of publisher: {}".format (self.pubnum))
            self.logger.info ("     Num of subscriber: {}".format (self.subnum))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e
        
###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Discovery Application")
    
    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)
    
    parser.add_argument ("-n", "--name", default="discovery", help=":D")

    parser.add_argument ("-S", "--subnum", type=int, default="1", help="total number of subscribers")

    parser.add_argument ("-P", "--pubnum", type=int, default="1", help="total number of publishers")
    
    parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this discovery to advertise (default: localhost)")
    
    parser.add_argument ("-p", "--port", type=int, default=5555, help="Port number on which our underlying discovery ZMQ service runs, default=5555")
    
    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    return parser.parse_args()

###################################
#
# Main program
#
###################################
def main ():
    try:
      # obtain a system wide logger and initialize it to debug level to begin with
      logging.info ("Main - acquire a child logger and then log messages in the child")
      logger = logging.getLogger ("DiscoveryAppln")

      # first parse the arguments
      logger.debug ("Main: parse command line arguments")
      args = parseCmdLineArgs ()

      # reset the log level to as specified
      logger.debug ("Main: resetting log level to {}".format (args.loglevel))
      logger.setLevel (args.loglevel)
      logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

      # Obtain a discovery application
      logger.debug ("Main: obtain the discovery appln object")
      disc_app = DiscoveryAppln (logger)

      # configure the object
      logger.debug ("Main: configure the discovery appln object")
      disc_app.configure (args)

      # now invoke the driver program
      logger.debug ("Main: invoke the discovery appln driver")
      disc_app.driver ()

    except Exception as e:
      logger.error ("Exception caught in main - {}".format (e))
      return

    
###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


  main ()