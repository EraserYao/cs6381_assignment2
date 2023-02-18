###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import time   # for sleep
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

class BrokerAppln():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER=2,
        ISREADY = 3,
        DISSEMINATE = 4,
        LOOKUP = 5
        

    def __init__(self,logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.num_topics=0
        self.topiclist = None # the different topics
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements

    def configure(self,args):
        try:
            self.logger.info ("BrokerAppln::configure")
            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE
            # initialize our variables
            self.name = args.name # our name
            self.num_topics = args.num_topics

            # Now get our topic list of interest
            self.logger.debug ("BrokerAppln::configure - selecting our topic list")
            ts = TopicSelector ()
            self.topiclist = ts.interest (self.num_topics)  # let topic selector give us the desired num of topics
    
            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug ("BrokerAppln::configure - initialize the middleware object")
            self.mw_obj = BrokerMW (self.logger)
            self.mw_obj.configure (args) # pass remainder of the args to the m/w object

            self.logger.info ("BrokerAppln::configure - configuration complete")
      
        except Exception as e:
            raise e
        
    def driver (self):
        ''' Driver program '''

        try:
            self.logger.info ("BrokerAppln::driver")

            self.dump ()

            self.logger.debug ("BrokerAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle (self)

            self.state = self.State.READY

            self.mw_obj.event_loop (timeout=0)  # start the event loop

            self.logger.info ("BrokerAppln::driver completed")

        except Exception as e:
            raise e
        
    def invoke_operation (self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info ("BrokerAppln::invoke_operation")

            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service
                self.logger.debug ("BrokerAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register (self.name,self.topiclist)

                return None

            elif (self.state == self.State.ISREADY):
                self.logger.debug ("BrokerAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready ()  # send the is_ready? request

                return None
      
            elif (self.state == self.State.LOOKUP):

                self.logger.debug ("BrokerAppln::invoke_operation - look up from discovery about publishers") 
                self.mw_obj.lookall_publisher() #send look up request

                return None
            
            elif (self.state == self.State.DISSEMINATE):
                self.logger.debug ("BrokerAppln::invoke_operation - start Disseminating")
        
                return None

            else:
                raise ValueError ("Undefined state of the appln object")

        except Exception as e:
            raise e

    def register_response (self, reg_resp):
        ''' handle register response '''
        try:
            self.logger.info ("BrokerAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("BrokerAppln::register_response - registration is a success")

                # set our next state to loojk up the publishers in discovery
                self.state = self.State.LOOKUP

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug ("BrokerAppln::register_response - registration is a failure")
                raise ValueError ("Broker needs to have unique id")

        except Exception as e:
            raise e

    def isready_response (self, isready_resp):
        ''' handle isready response '''

        try:
            self.logger.info ("BrokerAppln::isready_response")

            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.
            if not isready_resp.status:
                # discovery service is not ready yet
                self.logger.debug ("BrokerAppln::driver - Not ready yet; check again")
                time.sleep (10)  # sleep between calls so that we don't make excessive calls

            else:
                # we got the go ahead
                # set the state to disseminate
                self.state = self.State.LOOKUP

            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e:
          raise e
    
    def lookall_response(self,lookall_resp):
        '''handle discovery response about publishers'''
        try:
            self.logger.info ("BrokerAppln::lookall_response")
            #return publishers which send topic to us
            for publisherInfo in lookall_resp.publisherInfos:
                # temporary store publishers in this structure
                self.mw_obj.connect_pub(str(publisherInfo.addr)+':'+str(publisherInfo.port))

            self.state = self.State.DISSEMINATE
            # return a timeout of zero so that the event loop in its next iteration will immediately make
            # an upcall to us
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
            self.logger.info ("BrokerAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Num Topics: {}".format (self.num_topics))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("**********************************")

        except Exception as e:
          raise e
        
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Subscriber Application")
    
    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)
    
    parser.add_argument ("-n", "--name", default="broker", help="Some name assigned to us. Keep it unique per subscriber")
    
    parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this subscriber to advertise (default: localhost)")
    
    parser.add_argument ("-p", "--port", type=int, default=5500, help="Port number on which our underlying subscriber ZMQ service runs, default=5500")
      
    parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")
    
    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")
    
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
      logger = logging.getLogger ("BrokerAppln")

      # first parse the arguments
      logger.debug ("Main: parse command line arguments")
      args = parseCmdLineArgs ()

      # reset the log level to as specified
      logger.debug ("Main: resetting log level to {}".format (args.loglevel))
      logger.setLevel (args.loglevel)
      logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

      # Obtain a subscriber application
      logger.debug ("Main: obtain the subscriber appln object")
      brk_app = BrokerAppln (logger)

      # configure the object
      logger.debug ("Main: configure the subscriber appln object")
      brk_app.configure (args)

      # now invoke the driver program
      logger.debug ("Main: invoke the subscriber appln driver")
      brk_app.driver ()

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