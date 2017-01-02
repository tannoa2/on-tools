import requests
import json, time, sys
import signal
from config.amqp import *
from modules.amqp import AMQPWorker
from proboscis.asserts import *
from threading import Timer,Thread
from decimal import *
import textwrap
import argparse
import time
import sys
from modules.worker import WorkerThread, WorkerTasks
from argparse import RawTextHelpFormatter
import matplotlib.pyplot as plt


throughput= 0.0
agrigateThroughput = 0.0
throuputARR = []
start_time = 0
done = False
max_wait = 0.0
time_to_clear_queue= 0.1 #in seconds
consumed_workflows = []
produced_workflows = []
dropped_workflows = []
started_posting = False
done_recusively_queue_clearing = True
cleared_amqp_msgs = 0

def signal_handler(signum,stack):
    global done
    print '  exiting..'
    done = True
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

def handle_graph_finish(body, message):
    global consumed_workflows, produced_workflows, max_wait, done_recusively_queue_clearing, cleared_amqp_msgs

    if (started_posting == True):
        temp_dict = {}
        routeId = message.delivery_info.get('routing_key').split('graph.finished.')[1]
        assert_not_equal(routeId, None)
        message.ack()
        temp_dict['time_stamp']= time.time()
        temp_dict['graph_id']= routeId
        consumed_workflows.append(temp_dict)
        graph_wait = 0
        for index, item in enumerate(produced_workflows):
            if item["graph_id"] == routeId:
                graph_wait =  temp_dict['time_stamp'] - item['time_stamp']
                break
        if (max_wait < graph_wait):
            max_wait = graph_wait
    else:
        cleared_amqp_msgs = cleared_amqp_msgs + 1
        done_recusively_queue_clearing = False
        message.ack()


def post_function(TOTAL_WORKFLOWS, API):
    global consumed_workflows, produced_workflows, dropped_workflow, start_time, started_posting
    started_posting = True
    start_time = time.time()
    for n in range(TOTAL_WORKFLOWS):
        temp_dict= {}
        r = post('/workflows?name=Graph.noop-example',API)
        graphId =  json.loads(r._content)["instanceId"]
        temp_dict['time_stamp'] = time.time()
        temp_dict['graph_id'] = graphId


        if (r.status_code != 201):
            dropped_workflows.append(temp_dict)
        else:
            produced_workflows.append(temp_dict)

def print_function(REFRESH_RATE):
    global done
    while 1:
        if(done == False):
            cw_length = len(consumed_workflows)
            pw_length = len(produced_workflows)
            dw_length = len(dropped_workflows)

            if(type(agrigateThroughput) == float and type(throughput)  == float and type(max_wait) == float):
                agrigateThroughput1 = "%.2f" % agrigateThroughput
                throughput1 = "%.2f" % throughput
                max_wait1 = "%.2f" % max_wait
                print ("\r PostedWFs:{0} FinishedWFs:{1} DroppedWFs:{2} Tph:{3}wf/s avgTph:{4}wf/s max_wait={5}sec".
                       format(pw_length, cw_length, dw_length, throughput1, agrigateThroughput1, max_wait1)),
                sleep_time = 1.0 / REFRESH_RATE
                time.sleep(sleep_time)
        else:
            break

def analyze_function(TOTAL_WORKFLOWS, SAMPLING_WINDOW):
    global consumed_workflows, produced_workflows, dropped_workflows, throughput, agrigateThroughput, throuputARR
    global start_time, done
    start_time = time.time()
    lastTime = time.time()
    last_consumed_workflows = len(consumed_workflows)
    while 1:
        currentTime = time.time()
        deltaTime = currentTime -lastTime

        cw_length = len(consumed_workflows)
        pw_length = len(produced_workflows)
        deltaWorkflows = cw_length - last_consumed_workflows

        if( deltaTime >= SAMPLING_WINDOW and deltaWorkflows > 0 ):
            lastTime = currentTime
            throughput =  deltaWorkflows / (deltaTime)
            throuputARR.append(float("%.2f" % throughput))
            last_consumed_workflows = cw_length
            agrigateThroughput = cw_length/ (currentTime - start_time)


        if( cw_length == TOTAL_WORKFLOWS):
            amqp_listner_worker.stop()
            break

if __name__ == '__main__':
    if len(sys.argv) >= 0:
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter, description=
        """A performance tool to calcualte the throughput of workflows/sec processed by RackHD. The output looks like the following:
        PostedWFs:13 FinishedWFs:14 DroppedWFs:0 Tph:0.00wf/s avgTph:0.00wf/s max_wait=0.00sec

        PostedWFs: is the number of workflows that have been posted to RackHD
        FinishedWFs: Number of workflows that has been proccessed by RackHD
        Tph: Troughput, which is the number of workflows/sec that are being proccessed by RackHD
        Tph: Troughput, which is the number of workflows/sec that are being proccessed by RackHD
        avgTph: Average or aggregate throughput
        max_wait: Number of Seconds that the longest workflow had to wait in the queue before it got proccessed
        """)
        parser.add_argument('-RR','--refresh_rate', type=int, default=15, required=False,
                            help="The refresh rate of the screen(per sec), default value is 15")
        parser.add_argument('-TW','--total_workflows', type=int, default=20, required=False,
                            help="Total number of workflows that will be posted, default value is: 20")
        parser.add_argument('-H','--host', default='localhost:8080', required=False,
                            help="RackHD IP:PORT, default is: localhost:8080 ")
        parser.add_argument('-SW','--sampling_window', type=int, default=3.0, required=False,
                            help="The period over which it is used to calculate the throughput, default value: 3.0 sec")
        parser.add_argument('-A', '--api', default="1.1", required=False,
                            help="Desired RackHD api")
        args = parser.parse_args()

        REFRESH_RATE = args.refresh_rate
        TOTAL_WORKFLOWS = args.total_workflows
        HOST = args.host
        SAMPLING_WINDOW = args.sampling_window
        API = args.api

    amqp_listner_worker = AMQPWorker(queue=QUEUE_GRAPH_FINISH, callbacks=[handle_graph_finish])
    BASE_URL = 'http://{0}/api/{1}'.format(HOST, API)

    def post(path, API,  data=None, headers=None):
        r = BASE_URL + path
        headers= {'Content-Type': 'application/json'}
        return requests.post(r, data, headers=headers)

    def thread_func(worker, id):
        worker.start()

    def run():
        post_worker = Thread(target=post_function,args=(TOTAL_WORKFLOWS, API))
        analyzer_worker = Thread(target=analyze_function, args=(TOTAL_WORKFLOWS,SAMPLING_WINDOW))
        printing_worker = Thread(target=print_function, args=[REFRESH_RATE])
        printing_worker.daemon = True
        analyzer_worker.daemon = True
        post_worker.daemon = True
        printing_worker.start()
        analyzer_worker.start()
        post_worker.start()
        amqp_listner_worker.start()


    def clear_queue(counter):
        global done_recusively_queue_clearing
        task = WorkerThread(amqp_listner_worker, 'amqp')
        tasks = WorkerTasks(tasks=[task], func=thread_func)
        tasks.run()
        tasks.wait_for_completion(time_to_clear_queue)

        if(counter < 10 and done_recusively_queue_clearing == False):
            done_recusively_queue_clearing = True
            counter = counter + 1
            clear_queue(counter)

    def plot():
        x2 = range(len(throuputARR))
        plot1, = plt.plot(x2, throuputARR, 'r')
        plt.title("Performance workflows/second")
        print throuputARR
        plt.show()

    counter = 0
    start = time.time()
    print'Clearing the graph.finished queue...'
    clear_queue(counter)
    end = time.time()
    print "cleared " + str(cleared_amqp_msgs) + " amqp message(s) in " + str(end-start) + " sec"
    print "running ...."
    run()
    plot()

    #sys.exit(0)


