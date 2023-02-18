###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.
# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

from topic_selector import TopicSelector
# Now import our CS6381 Middleware
from CS6381_MW.SubscriberMW import SubscriberMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

class SubscriberAppln():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        LOOKUP = 3,
        DATARECEIVE = 4

    def __init__(self,logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics
        #self.iters = None   # number of iterations of publication
        #self.frequency = None # rate at which dissemination takes place
        self.num_topics = None # total num of topics we want to receive
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements

    def configure(self,args):
        try:
            self.logger.info ("SubscriberAppln::configure")
            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE
            # initialize our variables
            self.name = args.name # our name
            #self.iters = args.iters  # num of iterations
            #self.frequency = args.frequency # frequency with which topics are disseminated
            self.num_topics = args.num_topics  # total num of topics we receive

            # Now, get the configuration object
            self.logger.debug ("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now get our topic list of interest
            self.logger.debug ("SubscriberAppln::configure - selecting our topic list")
            ts = TopicSelector ()
            self.topiclist = ts.interest (self.num_topics)  # let topic selector give us the desired num of topics
    
            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug ("SubscriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW (self.logger)
            self.mw_obj.configure (args) # pass remainder of the args to the m/w object

            self.logger.info ("SubscriberAppln::configure - configuration complete")
      
        except Exception as e:
            raise e


    ########################################
    # driver program
    ########################################
    def driver (self):
        ''' Driver program '''

        try:
            self.logger.info ("SubscriberAppln::driver")

            self.dump ()

            self.logger.debug ("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle (self)

            self.state = self.State.REGISTER

            self.mw_obj.event_loop (timeout=0)  # start the event loop

            self.logger.info ("SubscriberAppln::driver completed")

        except Exception as e:
            raise e


    def invoke_operation (self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info ("SubscriberAppln::invoke_operation")

            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service
                self.logger.debug ("SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register (self.name,self.topiclist)

                return None

            elif (self.state == self.State.LOOKUP):

                self.logger.debug ("SubscriberAppln::invoke_operation - look up from discovery about publishers") 
                self.mw_obj.lookup_publisher(self.topiclist) #send look up request
                
                return None
            
            elif (self.state == self.State.DATARECEIVE):
                
                self.logger.debug ("SubscriberAppln::invoke_operation - connect to publisher and reveive data")    

                return None

            else:
                raise ValueError ("Undefined state of the appln object")

        except Exception as e:
            raise e

    def register_response (self, reg_resp):
        ''' handle register response '''
        try:
            self.logger.info ("SubscriberAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("SubscriberAppln::register_response - registration is a success")

                # set our next state to loojk up the publishers in discovery
                self.state = self.State.LOOKUP

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug ("SubscriberAppln::register_response - registration is a failure")
                raise ValueError ("Subscriber needs to have unique id")

        except Exception as e:
            raise e


    def lookup_response(self,lookup_resp):
        '''handle discovery response about publishers'''
        try:
            self.logger.info ("SubscriberAppln::lookup_response")
            if (lookup_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("SubscriberAppln::lookup_response - start receive publisher")
                
                #return publishers which send topic to us
                for publisherInfo in lookup_resp.publisherInfos:
                    # temporary store publishers in this structure
                    self.mw_obj.connect_pub(str(publisherInfo.addr)+':'+str(publisherInfo.port))
                #next step is to connect all these publishers

                self.state = self.State.DATARECEIVE

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug ("SubscriberAppln::lookup_response - look up failure")
                raise ValueError ("Subscriber looks up failure")
            
        except Exception as e:
            raise e
        
    def data_receive(self,received_data):
        try:

            strs=received_data.split(':')
            #print data we received
            self.print_data(strs[0],strs[1])
            self.state = self.State.LOOKUP
            return 0
        except Exception as e:
            raise e
    
    ########################################
    # dump the contents of the object 
    ########################################
    def dump (self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("SubscriberAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("     Num Topics: {}".format (self.num_topics))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e
        
    def print_data (self, topic, content):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("SubscriberAppln::print")
            self.logger.info ("------------------------------")
            self.logger.info ("     Topic: {}".format (topic))
            self.logger.info ("     Content: {}".format (content))
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
    parser = argparse.ArgumentParser (description="Subscriber Application")
    
    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)
    
    parser.add_argument ("-n", "--name", default="sub", help="Some name assigned to us. Keep it unique per subscriber")
    
    parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this subscriber to advertise (default: localhost)")
    
    parser.add_argument ("-p", "--port", type=int, default=5677, help="Port number on which our underlying subscriber ZMQ service runs, default=5677")
      
    parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")
    
    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    
    parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    
    parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

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
      logger = logging.getLogger ("SubscriberAppln")

      # first parse the arguments
      logger.debug ("Main: parse command line arguments")
      args = parseCmdLineArgs ()

      # reset the log level to as specified
      logger.debug ("Main: resetting log level to {}".format (args.loglevel))
      logger.setLevel (args.loglevel)
      logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

      # Obtain a subscriber application
      logger.debug ("Main: obtain the subscriber appln object")
      sub_app = SubscriberAppln (logger)

      # configure the object
      logger.debug ("Main: configure the subscriber appln object")
      sub_app.configure (args)

      # now invoke the driver program
      logger.debug ("Main: invoke the subscriber appln driver")
      sub_app.driver ()

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