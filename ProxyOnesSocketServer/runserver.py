from server import app

if __name__ == '__main__':
    HOST = '192.168.777.222'
    PORT = 8095

    app.run(HOST, PORT, threaded=True) 

