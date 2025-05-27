from config import load_config

class DbConfig():

    def __init__(self,filename='database.ini', section='postgresql'):
        self.filename = filename
        self.section = section
        self.config = None
        self.schema = None

    def load_dbconfig(self):
        self.config = load_config(filename=self.filename,section=self.section)
        self.schema = self.config.get('schema','')
        self.user = self.config['user']
        self.password = self.config['password']
        self.host = self.config['host']
        self.service = self.config.get('service',None)
        self.port = self.config['port']

        return self.config

    def load_queryconfig(self):
        self.config = load_config(filename=self.filename,section=self.section)
        return self.config

