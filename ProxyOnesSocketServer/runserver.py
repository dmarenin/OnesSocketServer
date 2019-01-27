from server import app

if __name__ == '__main__':
    HOST = '192.168.7.220'
    PORT = 8095

    app.run(HOST, PORT, threaded=True) 

