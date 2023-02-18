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
        READY = 3,
        ISREADY = 4,
        DISSEMINATE = 5,
        COMPLETED = 6,
        LOOKUP = 7,
        DATARECEIVE = 8
        

    def __init__(self,logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
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
                self.mw_obj.register (self.name)

                return None

            elif (self.state == self.State.ISREADY):
                self.logger.debug ("BrokerAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready ()  # send the is_ready? request

                return None
      
            elif (self.state == self.State.DISSEMINATE):
                self.logger.debug ("BrokerAppln::invoke_operation - start Disseminating")

                # Now disseminate topics at the rate at which we have configured ourselves.
                ts = TopicSelector ()
                for i in range (self.iters):
                    for topic in self.topiclist:
                      # For now, we have chosen to send info in the form "topic name: topic value"
                      # In later assignments, we should be using more complex encodings using
                      # protobuf.  In fact, I am going to do this once my basic logic is working.
                        dissemination_data = ts.gen_publication (topic)
                        self.mw_obj.disseminate (self.name, topic, dissemination_data)

                    # Now sleep for an interval of time to ensure we disseminate at the
                    # frequency that was configured.
                    time.sleep (1/float (self.frequency))  # ensure we get a floating point num

                self.logger.debug ("BrokerAppln::invoke_operation - Dissemination completed")

                # we are done. So we move to the completed state
                self.state = self.State.COMPLETED

                # return a timeout of zero so that the event loop sends control back to us right away.
                return 0

            elif (self.state == self.State.LOOKUP):

                self.logger.debug ("BrokerAppln::invoke_operation - look up from discovery about publishers") 
                self.mw_obj.lookup_publisher(self.topiclist) #send look up request

                return None
            
            elif (self.state == self.State.DATARECEIVE):
                
                self.logger.debug ("BrokerAppln::invoke_operation - connect to publisher and reveive data")    
                #recevie the message
                for pubaddr in self.pubinfos.values():
                    received_data = self.mw_obj.receive_data (self.name,pubaddr)
                    strs=received_data.split(':')
                    #print data we received
                    self.print_data(strs[0],strs[1])


                self.logger.debug ("BrokerAppln::invoke_operation - date receive completed")
    
                # we are done. And continue to receive publishers
                self.state = self.State.LOOKUP
    
                return 0

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
                raise ValueError ("Subscriber needs to have unique id")

        except Exception as e:
            raise e


    def lookup_response(self,lookup_resp):
        '''handle discovery response about publishers'''
        try:
            self.logger.info ("BrokerAppln::lookup_response")
            if (lookup_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("BrokerAppln::lookup_response - start receive publisher")
                
                #return publishers which send topic to us
                for publisherInfo in lookup_resp.publisherInfos:
                    # temporary store publishers in this structure
                    self.pubinfos[str(publisherInfo.id)]=str(publisherInfo.addr)+':'+str(publisherInfo.port)

                #next step is to connect all these publishers

                self.state = self.State.DATARECEIVE

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug ("BrokerAppln::lookup_response - look up failure")
                raise ValueError ("Subscriber looks up failure")
            
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
    
    parser.add_argument ("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")

    parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument ("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    
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