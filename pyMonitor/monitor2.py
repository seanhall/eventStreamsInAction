import datetime, socket, uuid, sys
print(sys.version)

def get_agent_version():
  return "0.1.0"

def get_hostname():
  return socket.gethostname()

def get_event_time():
  return datetime.datetime.now().isoformat()

def get_event_id():
  return str(uuid.uuid4())

# print "agent: {}, hostname: {}, time: {}, id: {}".format(get_agent_version(), get_hostname(), get_event_time(), get_event_id())
print(f"{get_agent_version()}, hostname: {get_hostname()}, time: {get_event_time()}, id: {get_event_id()}")