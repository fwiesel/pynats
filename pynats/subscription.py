class Subscription(object):
    def __init__(self, sid, subject, queue, callback, connection):
        self.sid = sid
        self.subject = subject
        self.queue = queue
        self.connection = connection
        self.callback = callback
        self.received = 0
        self.delivered = 0
        self.bytes = 0
        self.max = 0

    def handle_msg(self, msg):
        self.callback(msg)
        return None
