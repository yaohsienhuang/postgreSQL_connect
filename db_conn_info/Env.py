import os

class env(object):
    @staticmethod
    def host():
        return os.environ.get('host')
    
    @staticmethod
    def port():
        return os.environ.get('port')
		
    @staticmethod
    def acc():
        return os.environ.get('acc')
		
    @staticmethod
    def pw():
        return os.environ.get('pw')
		
    @staticmethod
    def sid():
        return os.environ.get('sid')
		
    @staticmethod
    def sslmode():
        return os.environ.get('sslmode')