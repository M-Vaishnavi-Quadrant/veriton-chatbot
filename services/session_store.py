class SessionStore:
    def __init__(self):
        self.sessions = {}

    def create(self, session_id, data):
        self.sessions[session_id] = data

    def get(self, session_id):
        return self.sessions.get(session_id)

    def update(self, session_id, data):
        self.sessions[session_id] = data

    def delete(self, session_id):
        self.sessions.pop(session_id, None)


session_store = SessionStore()