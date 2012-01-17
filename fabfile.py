import os
import time
import socket
from itertools import product
from fabric.api import *
import boto.ec2

AMI = 'ami-6bb5821f'
KEY_NAME = 'ec2-mobverdb-ssh'
SECURITY_GROUPS = ('mobverdb',)

JAR = 'sorting-runnable.jar'
FILE = 'faust.txt'

env.user = 'root'
env.key_filename = os.path.expanduser('~/'+KEY_NAME+'.pem')

def test_mergesort(clients, sizes):
    '''Test the performance of mergesort.
    Parameter 'clients' and 'sizes' are expected to be strings
    that contain space separated integers (since fabric doesn't allow list-arguments).
    For each combination of size and client a test run will be done.
    
    Example:
        fab test_mergesort:"1","10 100"
        
        will execute a test with one client and blocksizes 10 and 100'''
    _run_test('m', clients, sizes)
    
def test_distsort(clients, sizes):
    '''Test the performance of distribution sort.
    Parameter 'clients' and 'sizes' are expected to be strings
    that contain space separated integers (since fabric doesn't allow list-arguments).
    For each combination of size and client a test run will be done.
    
    Example:
        fab test_mergesort:"1","10 100"
        
        will execute a test with one client and blocksizes 10 and 100'''
    _run_test('d', clients, sizes)
    
def test_localsort():
    '''Test the performance of local sorting.'''
    # request as many instances as needed (+1 is one for the server)        
    request(1)
    # start server
    execute(start_server, 'l', 0, False)
    # get log and store it locally
    execute(fetch_perflog, 'l', 0, 0)

def _run_test(mode, clients, sizes):
    
    for cc, bs in product(clients.split(' '), sizes.split(' ')):
        client_count = int(cc)
        blocksize = int(bs)

        # request as many instances as needed (+1 is one for the server)        
        request(client_count+1)
        # start server
        execute(start_server, mode, blocksize)
        # start clients. this will wait until all clients terminate
        execute(start_clients)
        # get log and store it locally
        execute(fetch_perflog, mode, client_count, blocksize)
  
def _kill_java():
    with settings(warn_only=True):
        # kill client if running
        run('pgrep java && kill $(pgrep java); sleep 1')

@roles('client')
@parallel
def start_clients():
    '''start client (parallel) and let them connect to server'''
    cmd = ('java -jar', JAR, '-c', env.roledefs['server'][0])
    
    _kill_java()
    run(' '.join(map(str, cmd)))

@roles('server')
def start_server(mode, blocksize=10, bg=True):
    '''start serverprocess (in background via dtach)'''
    if bg:
        cmd = ['dtach -n /tmp/sorting -Ez']
    else:
        cmd = []
    if mode is 'd':
        cmd += ['java -jar', JAR, '-s', len(env.roledefs['client']), '-d', '-b', blocksize, FILE]
    else:
        cmd += ['java -jar', JAR, '-s', len(env.roledefs['client']), '-b', blocksize, FILE]
    
    _kill_java()
    run(' '.join(map(str, cmd)))        

@roles('server')
def fetch_perflog(prefix, client_count, blocksize):
    '''fetch der perf.log file from the server and store it locally
    (renamed be clientcount and blocksize)'''
    get('perf.log', '%s_perf_%s_%s.log'%(prefix, client_count, blocksize))


    
    
def request(num):
    '''request 'num' running instances on ec2. if there are to few new ones will created.
    stopped instances will be started'''
    conn = boto.ec2.connect_to_region('eu-west-1')
    
    # get all existing with the right id and start stopped ones
    all = []
    for r in conn.get_all_instances():
        for i in r.instances:
            if i.image_id == AMI:
                if i.state == 'stopped':
                    if len(all) < num:
                        i.start()
                        print "starting", i
                        all.append(i)
                    else:
                        break
                elif i.state == 'running':
                    all.append(i)
                    
    # wait until every instance runs
    for instance in all:
        _wait_for_instance(instance)           
    
    # if there are less then required, create new ones
    diff = int(num) - len(all)          
    if diff > 0:
        print "more instances needed. creating", diff
        newly_created = create_ec2(diff)
        all += newly_created
    else:
        print "enough available"
    
    # select only as much as needed    
    used = all[:int(num)]
    
    # inject them into the environment
    server = used[0]
    clients = used[1:]
    env.hosts = [i.public_dns_name for i in used ]
    env.roledefs = {
        'server': [server.public_dns_name,],
        'client': [i.public_dns_name for i in clients ]
    }    

def create_ec2(num):
    '''create 'num' new instances and install the environment on them'''
    conn = boto.ec2.connect_to_region('eu-west-1')
    res = conn.run_instances(AMI, instance_type='t1.micro', key_name=KEY_NAME, security_groups=SECURITY_GROUPS, max_count=num)
           
    all_running = False
   
    for instance in res.instances:
        _wait_for_instance(instance)                

    # deploy needed environment to newly created instances
    with settings(hosts=[i.public_dns_name for i in res.instances ]):
        execute(install_environment)
        execute(copy_files)
        
    return res.instances
   
def copy_files():
    '''copy jar and data file onto hosts (parallel)'''
    put('target/'+JAR, JAR)
    put(FILE, FILE)  

@parallel(pool_size=3) 
def install_environment():
    '''install the needed software on the hosts'''
    run('aptitude -q -y update > /dev/null')
    run('aptitude -q -y install openjdk-6-jre-headless dtach > /dev/null')
    
def give_shell():
    open_shell()        
    
def stop():
    conn = boto.ec2.connect_to_region('eu-west-1')
    for res in conn.get_all_instances():
        for instance in res.instances:
            if instance.state == 'running' and instance.image_id == AMI:
                print "stopping", instance
                instance.stop()

def _wait_for_instance(instance):
    while True:
        if instance.update() == 'running' and instance.public_dns_name != '' and _test_ssh(instance.public_dns_name):
            return
        else:
            time.sleep(3)
    
def _test_ssh(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(1)
        sock.connect((address, 22))
        return True
    except Exception:
        pass
    finally:
        sock.close()
    return False