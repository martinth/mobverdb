import os
import os.path
import time
import socket
from itertools import product
from fabric.api import *
import boto.ec2

AMI = 'ami-6bb5821f'
KEY_NAME = 'ec2-mobverdb-ssh'
SECURITY_GROUPS = ('mobverdb',)


JOIN_JAR = 'joiner-runnable.jar'
JOIN_FILES = ('join-data-a.csv', 'join-data-b.csv', 'join-data-base.csv') 
SORT_JAR = 'sorting-runnable.jar'
SORT_FILE = 'faust.txt'

env.user = 'root'
env.key_filename = os.path.expanduser('~/'+KEY_NAME+'.pem')

#
# JOINING
#
def join_local():    
    '''local join on a single instance'''    
    request(1)
    execute(start_join_server, 'local', files=JOIN_FILES[:2], bg=False)
    execute(fetch_join_perflog, 'local')
    env.hosts = []
    
def join_shipwhole():    
    '''ship-whole join with two clients'''    
    request(3)
    mode = 'ship-whole'
    execute(start_join_server, mode, file=JOIN_FILES[2])
    execute(start_join_clients, mode, file=JOIN_FILES[0])
    execute(fetch_join_perflog, mode)
    env.hosts = []
   
def join_fetchasneeded():    
    '''fetch-as-needed join with one clients'''    
    request(2)
    mode = 'fetch-needed'
    execute(start_join_server, mode, file=JOIN_FILES[2])
    execute(start_join_clients, mode, file=JOIN_FILES[0])
    execute(fetch_join_perflog, mode)
    env.hosts = []
    
def join_bitvektor(sizes='10'):    
    '''bitvektor join with one clients. Paramter must be a space seperated list of blocksizes'''    
    request(2)
    mode = 'bitvektor'
    
    for s in sizes.split(' '):
        execute(start_join_server, mode, blocksize=s, file=JOIN_FILES[2])
        execute(start_join_clients, mode, file=JOIN_FILES[0])
        execute(fetch_join_perflog, mode, blocksize=s)
    env.hosts = []
    
def join_semi():    
    '''simple semi join with one clients'''    
    request(2)
    mode = 'semi'
    execute(start_join_server, mode, file=JOIN_FILES[2])
    execute(start_join_clients, mode, file=JOIN_FILES[0])
    execute(fetch_join_perflog, mode)
    env.hosts = []
    
def join_semiparallel():    
    '''parallel semi join with two clients'''    
    request(3)
    mode = 'semi-parallel'
    execute(start_join_server, mode, file=JOIN_FILES[2])
    execute(start_join_clients, mode, file=JOIN_FILES[0])
    execute(fetch_join_perflog, mode)
    env.hosts = []
    
def join_semisequential():    
    '''sequential semi join with two clients'''    
    request(3)
    mode = 'semi-sequential'
    execute(start_join_server, mode, file=JOIN_FILES[2])
    execute(start_join_clients, mode, file=JOIN_FILES[0])
    execute(fetch_join_perflog, mode)
    env.hosts = []
    
@roles('server')
def fetch_join_perflog(type, blocksize=None):
    output_name = 'perf_%s.log' % type
    if blocksize:
        output_name = 'perf_%s_%s.log' % (type, blocksize)
    get('perf.log', output_name)

@roles('server')
def start_join_server(mode, file='', files=(), blocksize=10, bg=True):
    '''start serverprocess (in background via dtach)'''
    
    if bg:
        cmd = ['dtach -n /tmp/sorting -Ez']
    else:
        cmd = []
    cmd += ['java -jar', JOIN_JAR, '--'+mode, '--blocksize', blocksize]
    if file:
        cmd += [file]
    elif files:
        cmd += files
    
    _kill_java()
    run(' '.join(map(str, cmd)))        


@roles('client')
@parallel
def start_join_clients(mode, file='', bg=False):
    '''start client (parallel) and let them connect to server'''
    _kill_java()
    
    if bg:
        cmd = ['dtach -n /tmp/sorting -Ez']
    else:
        cmd = []
    
    cmd += ('java -jar', JOIN_JAR, '--'+mode, '-c', env.roledefs['server'][0])
    
    if file:
        cmd += [file]
    run(' '.join(map(str, cmd)))
    
#
# SORTING
#

def test_mergesort(clients, sizes):
    '''Test the performance of mergesort.
    Parameter 'clients' and 'sizes' are expected to be strings
    that contain space separated integers (since fabric doesn't allow list-arguments).
    For each combination of size and client a test run will be done.
    
    Example:
        fab test_mergesort:"1","10 100"
        
        will execute a test with one client and blocksizes 10 and 100'''
    _run_sort_test('m', clients, sizes)
    
def test_distsort(clients, sizes):
    '''Test the performance of distribution sort.
    Parameter 'clients' and 'sizes' are expected to be strings
    that contain space separated integers (since fabric doesn't allow list-arguments).
    For each combination of size and client a test run will be done.
    
    Example:
        fab test_mergesort:"1","10 100"
        
        will execute a test with one client and blocksizes 10 and 100'''
    _run_sort_test('d', clients, sizes)
    
def test_localsort():
    '''Test the performance of local sorting.'''
    # request as many instances as needed (+1 is one for the server)        
    request(1)
    # start server
    execute(start_sort_server, 'l', 0, False)
    # get log and store it locally
    execute(fetch_sort_perflog, 'l', 0, 0)

def _run_sort_test(mode, clients, sizes):
    
    for cc, bs in product(clients.split(' '), sizes.split(' ')):
        client_count = int(cc)
        blocksize = int(bs)

        # request as many instances as needed (+1 is one for the server)        
        request(client_count+1)
        # start server
        execute(start_sort_server, mode, blocksize)
        # start clients. this will wait until all clients terminate
        execute(start_sort_clients)
        # get log and store it locally
        execute(fetch_sort_perflog, mode, client_count, blocksize)
  
def _kill_java():
    with settings(warn_only=True):
        # kill client if running
        run('pgrep java && kill $(pgrep java); sleep 1')

@roles('client')
@parallel
def start_sort_clients():
    '''start client (parallel) and let them connect to server'''
    cmd = ('java -jar', SORT_JAR, '-c', env.roledefs['server'][0])
    
    _kill_java()
    run(' '.join(map(str, cmd)))

@roles('server')
def start_sort_server(mode, blocksize=10, bg=True):
    '''start serverprocess (in background via dtach)'''
    if bg:
        cmd = ['dtach -n /tmp/sorting -Ez']
    else:
        cmd = []
    if mode is 'd':
        cmd += ['java -jar', SORT_JAR, '-s', len(env.roledefs['client']), '-d', '-b', blocksize, SORT_FILE]
    else:
        cmd += ['java -jar', SORT_JAR, '-s', len(env.roledefs['client']), '-b', blocksize, SORT_FILE]
    
    _kill_java()
    run(' '.join(map(str, cmd)))        

@roles('server')
def fetch_sort_perflog(prefix, client_count, blocksize):
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
    put('target/'+SORT_JAR, SORT_JAR)
    put(SORT_FILE, SORT_FILE)  
    put('target/'+JOIN_JAR, JOIN_JAR)
    for file in JOIN_FILES:
        put(file, file)

def copy_file(filename):
    '''copy a single file to all'''
    put(filename, os.path.basename(filename))

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
