import zmq
import json
import time


def send_request(action, data_list=[]):
    
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://192.168.29.2:5556")
    
    request_data = {
        "action": action,
        "data": data_list
    }

    print(f"Sending request....")
    socket.send_json(request_data)
    

    socket.close()
    context.term()

if __name__ == "__main__":
    
    send_request("delete device", [{
        "id": "3",
        "url": "video.mp4",
        "analytics": [
                    {"type":"loitering",
                            "roi":[
                                [100000, 106],
                                [15, 621],
                                [626, 600],
                                [613, 97]
                            ]
                            }, 
                    {"type":"intrusion",
                    "roi":[
                        [100200000, 150],
                        [110, 620],
                        [620, 600],
                        [600, 120]
                    ]
                }
                ],
        "name": "Camera 2",
        "fps": 2,
        "username": "admin123456",
        "password": "password123456"
    }])

    # time.sleep(2)

    # send_request("get_all")

    # time.sleep(2)

    # send_request("delete_device", [{"id": "stream_1"}])

    # time.sleep(2)

    # send_request("delete_all")