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
    
    send_request("delete analytic", [{
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
                            }
                #     {"type":"intrusion",
                #     "roi":[
                #         [100200000, 150],
                #         [110, 620],
                #         [620, 600],
                #         [600, 120]
                #     ]
                # }
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

#Import necessary libraries
# import numpy as np
# import base64
# import cv2
# import json
# import zmq
# import torch
# import time 
# import threading
# from concurrent.futures import ThreadPoolExecutor
# from datetime import datetime
# from ultralytics import YOLO
# from PersonTracker import Tracker
# import os
# import sys
# from pathlib import Path
# import queue
# from queue import Queue
# from pymongo import MongoClient

# #Global queue for all events
# alert_queue = Queue()

# gst_install_dir = Path(r"C:\gstreamer-python")
# gst_bin_dir = gst_install_dir / 'bin'

# sys.path.append(str(gst_install_dir / 'lib' / 'site-packages'))

# os.environ['PATH'] += os.pathsep + 'C:\\gstreamer-python\\lib\\gstreamer-1.0'
# os.environ['PATH']+=os.pathsep + 'C:\\GSTREAMER\\gstreamer\\build\\subprojects\\gstreamer\\libs\\gst\\helpers\\gst-plugin-scanner.exe'

# os.add_dll_directory(str(gst_bin_dir))

# import gi
# gi.require_version('Gst', '1.0')
# from gi.repository import Gst

# Gst.init(sys.argv)

# client = MongoClient("mongodb://localhost:27017/")
# db = client["Detection_Database"]
# collection = db["Alerts"]

# #Load config file
# def load_config(config_path = 'config.json'):
#     with open(config_path , 'r') as config_file:
#       return json.load(config_file) #return data in config file
    
# #Load classes_yolo file
# with open('classes_yolo.json' , 'r') as class_file:
#     data = json.load(class_file)
#     CLASS_NAMES = data['CLASS_NAMES']
      
# config = load_config() #Store in a variable

# #Initialize tracker
# person_tracker = Tracker()

# #Initialize ZMQ context(containers for all sockets)
# context = zmq.Context()
# #Choose socket from Context
# zmq_socket = context.socket(zmq.PUB) #Publisher for sending data
# #Takes address from config.json
# zmq_socket.connect(config['zmq']['publisher']['address'])

# #Initialize model
# device = torch.device('cuda' if torch.cuda.is_available() else 'cpu') #To run program on GPU
# model = YOLO('yolov8s.pt') #Detection Model
# model.to(device)
# fall_model = YOLO('fall.pt') #Fall Model
# fall_model.to(device)

# lock = threading.Lock() #Apply lock to avoid deadlocks
# json_lock = threading.Lock()
# pipeline_lock = threading.Lock()

# intrusion_classes = config['detection'].get('intrusion_classes', [0]) # 0-> for person , default value if no classes present in json
# max_cameras = config['detection'].get('max_cameras', 10) #10 -> default value for cameras if not present in json
# crowd_threshold = config['detection'].get('crowd_threshold')
# loitering_threshold = config['detection'].get('loitering_threshold')
# fall_threshold = config['detection'].get('fall_threshold')

# #Initialize all dictionaries and variables
# pipelines = {} #Dictionary to save pipelines based on id for flushing in case of deletion.
# stream_flags = {} #Marks the stream as active , keeps track of whether stream is running.
# streams_thread= {} #Keeps track of all threads
# entry_time = {}
# stream_fail_flags = {}
# last_restart_attempt = {}
# last_frame_times = {}
# latest_frames = {}

# #Load data from json file
# def load_streams_from_json(file_path):
#     try:
#       with open(file_path,'r') as file:
#           data = json.load(file)
          
#           #Create empty dictionaries and extract data from json file and put them seperately
#           streams = {}
#           analytics_dict = {}
#           stream_metadata = {}
          
#           for stream in data['streams']:
#               rtsp_id = stream['id']
#               streams[rtsp_id] = stream['url']
#               analytics_dict[rtsp_id] = stream.get('analytics',[])
              
#               #Save additional metadata
#               stream_metadata[rtsp_id]={
#                   "name":stream.get('name', f'Stream{rtsp_id}'),
#                   "fps":stream.get('fps', 2),
#                   "username":stream.get('username',''),
#                   "password":stream.get('password','')
#               }
#           return streams, analytics_dict, stream_metadata
#     except Exception as e:
#         print(f"Error loading JSON file: {e}")
#         return {}, {}, {}
    
# #Start stream using threadpoolexecutor
# def start_stream(rtsp_id,rtsp_url,analytics_dict,metadata,executor):
#     #Start the stream in a seperate thread
#     print(f"Starting thread for Stream ID: {rtsp_id} and {rtsp_url}", flush=True)
#     try:
#         stream_flags[rtsp_id] = True  # Set the flag to True to allow processing
#         stream_thread = executor.submit(process_stream, rtsp_id, rtsp_url, analytics_dict, metadata)
#         streams_thread[rtsp_id] = stream_thread
#     except Exception as e:
#         print(f"Failed to start Stream {rtsp_id}: {e}")
        
# def stop_stream(rtsp_id):
#     #Stop the thread for specific rtsp_id if running
#     print(f"Stopping thread for {rtsp_id}")
#     stream_flags[rtsp_id] = False
#     if rtsp_id in streams_thread:
#         future = streams_thread[rtsp_id]
#         future.result()
#         del streams_thread[rtsp_id]
#         #Flush the pipeline
#         pipeline = pipelines[rtsp_id]
#         pipeline.set_state(Gst.State.NULL)
#         del pipelines[rtsp_id] 
#         print("Pipeline flushed and thread stopped")
#         # cv2.destroyWindow(f"Stream {rtsp_id}")
#         # print(f"OpenCV window for Stream {rtsp_id} closed.")
        
# def is_inside_roi(center_points , roi_points):
#     if len(roi_points)>2:
#         result = cv2.pointPolygonTest(roi_points,center_points,False)
#         return result>=0
    
# def zmq_sender_and_save_in_mongodb():
#     socket = context.socket(zmq.PUB)
#     try:
#         socket.connect("tcp://127.0.0.1:5555")
#     except zmq.ZMQError as e:
#         print(f"ZMQ Connection Error: {e}")
#         return
#     try:
#         while True:
#             try:
#                 queue_time, payload = alert_queue.get()
#                 data_sent_to_mongodb = {
#                     "ID" : payload.get("ID"),
#                     "Event" : payload.get("Event"),
#                     "Timestamp" : payload.get("Timestamp")
#                 }
#                 get_time = time.time()
#                 #Calculate delay
#                 queue_delay_time = get_time - queue_time
#                 # print(f'Queue delay time: {queue_delay_time}')
#                 # print(f"Retrieved from alert_queue: {payload}")
#                 json_data = json.dumps(payload)
#                 socket.send_string(json_data)
#                 collection.insert_one(data_sent_to_mongodb)
#                 # print(f"Sent to ZMQ: {json_data}") 
#                 alert_queue.task_done() #Ensures item is processed and memory is released.
#                 del payload #Memory clean explicitly
#             except queue.Empty:
#                 pass #No data to send , retry loop
#     except Exception as e:
#         print(f"Error in zmq : {e}")

# def save_frame_and_send_intrusion_alert(rtsp_id,frame):
#         folder_path=f"./Intrusion/{datetime.now().strftime('%Y-%m-%d')}"
#         os.makedirs(folder_path,exist_ok=True)
#         filename = os.path.join(folder_path, f"{rtsp_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
#         cv2.imwrite(filename, frame)
#         _ , buffer = cv2.imencode('.jpg',frame)
#         img_base64 = base64.b64encode(buffer).decode('utf-8')
#         event_config = config['events']['intrusion']
#         payload = {
#             "ID":rtsp_id,
#             "Event":event_config.get('event_type'),
#             "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
#             "FrameData":img_base64,
#             "Detection":True
#         }
#         #Put payload in the queue
#         queue_time = time.time()
#         alert_queue.put((queue_time , payload))
    
# def save_frame_and_send_loitering_alert(rtsp_id,frame):
#         folder_path=f"./Loitering/{datetime.now().strftime('%Y-%m-%d')}"
#         os.makedirs(folder_path,exist_ok=True)
#         filename = os.path.join(folder_path, f"{rtsp_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
#         cv2.imwrite(filename, frame)
#         _ , buffer = cv2.imencode('.jpg',frame)
#         img_base64 = base64.b64encode(buffer).decode('utf-8')
#         event_config = config['events']['loitering']
#         payload = {
#             "ID":rtsp_id,
#             "FrameData":img_base64,
#             "Event":event_config.get('event_type'),
#             "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
#             "Detection":True
#         }
#         #Put payload in the queue
#         queue_time = time.time()
#         alert_queue.put((queue_time , payload))
        
# def save_frame_and_send_crowd_alert(rtsp_id,frame):
#         folder_path=f"./Crowd/{datetime.now().strftime('%Y-%m-%d')}"
#         os.makedirs(folder_path,exist_ok=True)
#         filename = os.path.join(folder_path, f"{rtsp_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
#         cv2.imwrite(filename, frame)
#         _ , buffer = cv2.imencode('.jpg',frame)
#         img_base64 = base64.b64encode(buffer).decode('utf-8')
#         event_config = config['events']['crowd_alert']
#         payload = {
#             "ID":rtsp_id,
#             "FrameData":img_base64,
#             "Event":event_config.get('event_type'),
#             "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
#             "Detection":True
#         }
#         #Put payload in the queue
#         queue_time = time.time()
#         alert_queue.put((queue_time , payload))
        
# def save_frame_and_send_fall_alert(rtsp_id,frame):
#         folder_path=f"./Fall/{datetime.now().strftime('%Y-%m-%d')}"
#         os.makedirs(folder_path,exist_ok=True)
#         filename = os.path.join(folder_path, f'{rtsp_id}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.jpg')
#         cv2.imwrite(filename, frame)
#         _ , buffer = cv2.imencode('.jpg',frame)
#         img_base64 = base64.b64encode(buffer).decode('utf-8')
#         event_config = config['events']['fall']
#         payload = {
#             "ID":rtsp_id,
#             "FrameData":img_base64,
#             "Event":event_config.get('event_type'),
#             "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
#             "Detection":True
#         }
#         #Put payload in the queue
#         queue_time = time.time()
#         alert_queue.put((queue_time , payload))
        
# def on_new_sample(sink, rtsp_id):
#     sample = sink.emit("pull-sample")
#     if not sample:
#         print(f"[{rtsp_id}] No sample received.")
#         return Gst.FlowReturn.ERROR

#     # Update timestamp and mark stream as active
#     with pipeline_lock:
#         last_frame_times[rtsp_id] = time.time()
#         stream_fail_flags[rtsp_id] = False  # Stream is working

#     print(f"Frame received for {rtsp_id}", flush=True)

#     # Extract buffer and caps
#     buf = sample.get_buffer()
#     caps = sample.get_caps()
#     if not caps:
#         print(f"[{rtsp_id}] No caps found in sample.")
#         return Gst.FlowReturn.ERROR

#     structure = caps.get_structure(0)
#     if not structure:
#         print(f"[{rtsp_id}] No structure found in caps.")
#         return Gst.FlowReturn.ERROR

#     height = structure.get_value("height")
#     width = structure.get_value("width")
#     if height is None or width is None:
#         print(f"[{rtsp_id}] Invalid height or width in caps.")
#         return Gst.FlowReturn.ERROR

#     # Map buffer to memory
#     success, map_info = buf.map(Gst.MapFlags.READ)
#     if not success:
#         print(f"[{rtsp_id}] Failed to map buffer.")
#         return Gst.FlowReturn.ERROR

#     try:
#         # Convert buffer to numpy array
#         frame = np.ndarray(
#             shape=(height, width, 3),
#             dtype=np.uint8,
#             buffer=map_info.data
#         )

#         # Store the frame in the latest_frames dictionary
#         with pipeline_lock:
#             latest_frames[rtsp_id] = frame

#     finally:
#         # Ensure buffer is unmapped
#         buf.unmap(map_info)

#     return Gst.FlowReturn.OK

# def on_pad_added(dbin, pad, convert):
#     # Ensure the pad from decodebin is linked to videoconvert
#     convert_pad = convert.get_static_pad("sink")
#     pad.link(convert_pad)
        
# def process_stream(rtsp_id,rtsp_url,analytics_dict,stream_metadata):
#     print(f"process_stream() started for Stream ID: {rtsp_id}",flush=True)
    
#     intrusion_roi = np.array(
#         next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "intrusion"), []), dtype=np.int32
#     ).reshape((-1, 1, 2))

#     crowd_roi = np.array(
#         next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "crowd"), []), dtype=np.int32
#     ).reshape((-1, 1, 2))

#     loitering_roi = np.array(
#         next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "loitering"), []), dtype=np.int32
#     ).reshape((-1, 1, 2))
    
#     fall_roi = np.array(
#         next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "fall"), []), dtype=np.int32
#     ).reshape((-1, 1, 2))
    
#     last_save_intrusion_time = 0  #Track last save intrusion time
#     last_save_loitering_time = 0 #Track last save loitering time
#     crowd_last_save_time = 0 #Track last save crowd time
#     fall_counter=0
#     fall_save_time = 0
    
#     fps = stream_metadata['fps']
#     # Define the GStreamer pipeline
#     #rtph264depay ! h264parse ! avdec_h264 !
#     gst_pipeline = (
#     f"rtspsrc location={rtsp_url} latency=500 !"
#     f"decodebin name=dec !"
#     f"videoconvert name=conv ! "
#     f"videoscale !"
#     f"videorate !"
#     f"video/x-raw,width=640,height=640,format=BGR,framerate={fps}/1 !"
#     f"appsink name=sink emit-signals=True max-buffers=1 drop=True"
#     )
#     print(f"GStreamer Pipeline for Stream {rtsp_id}: {gst_pipeline}")
#     try:
#         # Create and start the GStreamer pipeline
#         pipeline = Gst.parse_launch(gst_pipeline)
#         appsink = pipeline.get_by_name("sink")  # Get the appsink element
#         dec = pipeline.get_by_name("dec")
#         convert = pipeline.get_by_name("conv")

#         if not appsink:
#             print("Error: Appsink element not found in the pipeline!")
#             return
        
#         dec.connect("pad-added", on_pad_added)
#         appsink.connect("new-sample" , lambda sink: on_new_sample(sink,rtsp_id))

#         pipeline.set_state(Gst.State.PLAYING)  # Start the pipeline
#         # Track the pipeline
#         pipelines[rtsp_id] = pipeline  # Track the pipeline by rtsp_id
#         inactivity_timeout = 10  # Seconds without frames = consider as failed
#         retry_interval = 5       # Retry every 5 seconds after failure
#         # Run the stream processing loop while flag is True
#         while stream_flags[rtsp_id]:
#             # Pull frame sample
#             now = time.time()
#             last_time = last_frame_times.get(rtsp_id, 0)
#             failed = stream_fail_flags.get(rtsp_id, False)

#             # Detect failure if no frames for inactivity_timeout
#             #If no frames are received for inactivity_timeout(10) seconds, the stream is marked as failed by setting stream_fail_flags[rtsp_id] = True.
#             if not failed and now - last_time > inactivity_timeout:
#                 print(f"[{rtsp_id}] No frames in {inactivity_timeout}s. Marking as failed.")
#                 stream_fail_flags[rtsp_id] = True
#                 last_restart_attempt[rtsp_id] = 0  # Reset attempt timer
#             # if sample is None:
#             #     print(f"No frame received for {rtsp_id}. Retrying......")
#             #     time.sleep(5)
                
#              # If failed, retry every retry_interval seconds
#             while stream_fail_flags[rtsp_id]:
#                 if now - last_restart_attempt[rtsp_id] >= retry_interval:
#                     print(f"[{rtsp_id}] Attempting to restart pipeline...")
#                     try:
#                         pipeline.set_state(Gst.State.NULL)
#                         time.sleep(1)
#                         with pipeline_lock:   #
#                           latest_frames.pop(rtsp_id, None)  # Remove stale frames  #
#                           last_frame_times.pop(rtsp_id, None)
#                         pipeline.set_state(Gst.State.PLAYING)
#                         state_return, current_state, pending_state = pipeline.get_state(timeout=5 * Gst.SECOND)
#                         print(f"{current_state}")
#                         if current_state != Gst.State.PLAYING:  #
#                             print(f"[{rtsp_id}] Failed to transition to PLAYING state.")  #
#                             continue  # Retry or handle the error  #

#                         last_restart_attempt[rtsp_id] = now
                        
#                         time.sleep(10)
#                         if rtsp_id in last_frame_times: #and now - last_frame_times[rtsp_id] <= 5#
#                             print(f"[{rtsp_id}] Pipeline restarted successfully.")
#                             stream_fail_flags[rtsp_id] = False
#                         else:
#                             print(f"[{rtsp_id}] Pipeline restart failed. Retrying...")
#                     except Exception as e:
#                         print(f"Error restarting")
#                     # After restart, wait for frames to reset failure flag

#             #time.sleep(1)  # Sleep to avoid busy-loop
#             frame = latest_frames.get(rtsp_id)
#             if frame is not None:
#                 frame = cv2.resize(frame,(640,640))
#                 results = model(frame,classes=intrusion_classes,iou=0.25,conf=0.3,verbose=False)  #verbose produces cleaner outputs, suppress the logs
                    
#                 detected_objects = []  
#                 person_bboxes = []
                
#                 for result in results:
#                     for box in result.boxes:
#                         x1,y1,x2,y2 = map(int,box.xyxy[0])
#                         class_id = int(box.cls[0]) #class id
#                         class_name = CLASS_NAMES.get(str(class_id),"Unknown")
#                         detected_objects.append({"bbox":[x1,y1,x2,y2] , "class_id":class_id , "class_name":class_name})
#                         if class_id == 0:
#                             person_bboxes.append([x1,y1,x2,y2])
                    
#                 #INTRUSION DETECTION
#                 #Check detection is inside roi
#                 if "intrusion" in [analytic["type"] for analytic in analytics_dict]:
#                     for obj in detected_objects:
#                         x1, y1, x2, y2 = obj["bbox"]
#                         class_id = obj["class_id"]
#                         class_name = obj["class_name"]
#                         centers = ((x1 + x2) // 2, (y1 + y2) // 2)
#                         if is_inside_roi(centers,intrusion_roi) and class_id in intrusion_classes:
#                             cv2.rectangle(frame, (x1, y1), (x2, y2), (255, 0, 0), 2)
#                             text = f"{class_name}"
#                             (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
#                             cv2.rectangle(frame, (x1, y1 - text_h - 10), (x1 + text_w, y1), (0, 0, 0), -1)
#                             cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
#                             current_time = time.time()
#                             if current_time - last_save_intrusion_time >= 5:
#                                 save_frame_and_send_intrusion_alert(rtsp_id,frame)
#                                 last_save_intrusion_time = current_time
                    
#                 if "loitering" in [analytic["type"] for analytic in analytics_dict]:
#                     # Assign IDs using person_tracker
#                     objects_bbs_ids, _ = person_tracker.update(person_bboxes)
#                     # Track time and alert for intrusion and loitering
#                     current_time = time.time()
#                     for obj in objects_bbs_ids:
#                         x1,y1,x2,y2,person_id = obj
#                         w = x2-x1
#                         h = y2-y1
#                         cx, cy = x1 + w // 2, y1 + h // 2  # Calculate center of bounding box

#                         cv2.rectangle(frame, (x1, y1), (x1+w, y1+h), (0, 255, 0), 2)  # Bounding box
#                         text = f"person ID: {person_id}"  # Display person ID
#                         cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
#                         if is_inside_roi((cx,cy),loitering_roi):
#                         # Loitering alert for new entry into ROI
#                             if person_id not in entry_time:
#                                 entry_time[person_id] = current_time
#                                 #print(f"Person {person_id} entered ROI at {entry_time[person_id]}")
                                
#                             time_in_roi = current_time - entry_time[person_id]
#                             #print(f"Person {person_id} Time in ROI: {time_in_roi}")
#                             if time_in_roi >= loitering_threshold:
#                                 #print('Loitering detected')
#                                 current_time = time.time()
#                                 # Check if the last loitering alert was sent more than loitering_alert_interval ago
#                                 if current_time - last_save_loitering_time >= 5:
#                                     save_frame_and_send_loitering_alert(rtsp_id, frame)
#                                     last_save_loitering_time = current_time
#                         else:
#                             # Remove tracking if person leaves ROI
#                             if person_id in entry_time:
#                                 del entry_time[person_id]
                
#                 #CROWD DETECTION
#                 if "crowd" in [analytic["type"] for analytic in analytics_dict]:
#                     persons_in_roi = 0 #Initialise persons 
#                     for x1, y1, x2, y2 in person_bboxes:
#                         center = (int((x1 + x2) / 2), int((y1 + y2) / 2))
#                         if is_inside_roi(center,crowd_roi):
#                             persons_in_roi += 1  #Increase counter
#                             #print('Persons in ROI:',persons_in_roi)
#                             if persons_in_roi > crowd_threshold:
#                                 current_time = time.time()
#                                 if current_time - crowd_last_save_time >=5 :
#                                     save_frame_and_send_crowd_alert(rtsp_id,frame),
#                                     crowd_last_save_time = current_time
                                        
#             #FALL DETECTION
#                 if "fall" in [analytic["type"] for analytic in analytics_dict]:
#                     fall_results = fall_model.predict(frame,iou=0.25,conf=0.5,verbose = False)
#                     fall_detected = False #Flag to check fall is detected
#                     for result in fall_results:
#                         for box in result.boxes:
#                             x1,y1,x2,y2 = map(int,box.xyxy[0])
#                             cx , cy = x1 + x2 //2 , y1 + y2 //2
#                             cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
#                             text = "Fall Detected"
#                             (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
#                             cv2.rectangle(frame, (x1, y1 - text_h - 10), (x1 + text_w, y1), (0, 0, 0), -1)
#                             cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
#                             #print(f"Fall detected at: ({cx}, {cy}), Box: ({x1}, {y1}, {x2}, {y2})") 
#                             if is_inside_roi((cx,cy),fall_roi):
#                                 fall_detected = True
#                                 fall_counter+=1  #Increment the counter for consecutive fall detection
#                                 #If fall detected for consecutive fall_threshold frames
#                                 #print(fall_counter)
#                                 if fall_counter >= fall_threshold:
#                                     current_time = time.time()
                                    
#                                     #print(f"Fall detected and bounding box drawn at: ({x1}, {y1}, {x2}, {y2})") 
#                                     if current_time - fall_save_time >= 5:
#                                         save_frame_and_send_fall_alert(rtsp_id,frame)
#                                         fall_save_time = current_time
#                                         #Reset frame counter
#                                         fall_counter = 0 #After fall threshold 
#                                 else:
#                                     #Reset frame counter if no fall is detected (fall detected in consecutive 2 frames , after that no fall is detected , so make counter as 0).
#                                     fall_counter = 0
                        
#                 #cv2.polylines(frame, [loitering_roi], isClosed=True, color=(0,0,255), thickness=2)
#                 #cv2.polylines(frame, [crowd_roi], isClosed=True, color=(0,255,255), thickness=2)
#                 #cv2.polylines(frame, [intrusion_roi], isClosed=True, color=(0,255,255), thickness=2)
#                 #cv2.polylines(frame, [fall_roi], isClosed=True, color=(0,255,255), thickness=2)  
                        
#                 if frame is not None:
#                     window_name = f"Stream {rtsp_id}"
#                     cv2.imshow(window_name, frame)
#                 else:
#                     print(f"No valid frame for Stream {rtsp_id}") 

#                 # Exit on 'ESC' key
#                 if cv2.waitKey(1) & 0xFF == 27:
#                     cv2.destroyAllWindows()
#                     break

#     except Exception as e:
#         print(f"Pipeline creation failed: {e}") 

#     finally:
#         # Ensure the pipeline is stopped properly when exiting
#         pipeline.set_state(Gst.State.NULL)
        
# def save_streams_to_json(file_path,streams,analytics_dict,stream_metadata):
#     try:
#         with json_lock:
#             data = {"streams": []}
#             for rtsp_id, rtsp_url in streams.items():
#                 stream_data = {
#                     "id": rtsp_id,
#                     "url": rtsp_url,
#                     "analytics": analytics_dict.get(rtsp_id, []),
#                     "name": stream_metadata.get(rtsp_id, {}).get("name", f"Stream {rtsp_id}"),
#                     "fps": stream_metadata.get(rtsp_id, {}).get("fps", 3),
#                     "username": stream_metadata.get(rtsp_id, {}).get("username", ""),
#                     "password": stream_metadata.get(rtsp_id, {}).get("password", "")
#                 }
#                 data["streams"].append(stream_data)
        
#                  # Save data to file
#             with open(file_path, 'w') as file:
#                 json.dump(data, file, indent=4)
#             print(f"Data successfully saved to {file_path}")
#     except FileNotFoundError:
#         print(f"Error: File not found at {file_path}. Please check the file path.")
#     except Exception as e:
#         print(f"Failed to save JSON: {e}")

                
# def zmq_listener(streams,analytics_dict,stream_metadata,executor):
#     rep_socket = context.socket(zmq.REP)
#     try:
#         rep_socket.bind("tcp://*:5556")  
#         print('Starting listener thread')
#     except zmq.ZMQError as e:
#         print(f"Failed to bind socket: {e}")
#         return  # Exit if binding fails
#     while True:
#         try:
#             response = {"status" : "error" , "message" : "server error"}
            
#             #Receive and parse the message
#             message = rep_socket.recv_json()
#             print(f"Received message:{message}")
#             rep_socket.send_json({"status": "ok"})
            
#             action = message.get("action")
#             data_list = message.get("data",[])
            
#             with lock:
#                 if action == "add device":
#                     for data in data_list:
#                         rtsp_id = data.get("id")
#                         rtsp_url = data.get("url")
#                         analytics = data.get("analytics",[])
#                         name = data.get("name",f"Stream {rtsp_id}")
#                         fps = data.get("fps",3)
#                         username = data.get("username","")
#                         password = data.get("password","")
#                         if rtsp_id in streams:
#                             print(f"Stream with ID {rtsp_id} already exists.")
#                             response["status"] = "error"
#                             response["message"] = f"Stream with ID {rtsp_id} already exists."
#                         else:
#                             streams[rtsp_id] = rtsp_url
#                             analytics_dict[rtsp_id] = analytics
#                             stream_metadata[rtsp_id] = {
#                                 "name":name,
#                                 "fps":fps,
#                                 "username":username,
#                                 "password":password
#                             }
#                             stream_flags[rtsp_id] = True
#                         try:
#                             start_stream(rtsp_id, streams[rtsp_id], analytics, stream_metadata.get(rtsp_id,{}), executor)
#                             save_streams_to_json("streams.json",streams,analytics_dict,stream_metadata)
#                             print(f"Stream {rtsp_id} added with analytics: {analytics}")
#                             response["status"] = "success"
#                             response["message"] = f"Stream {rtsp_id} started."
#                         except Exception as e:
#                             response["status"] = "error"
#                             response["message"] = f"Error while submitting stream {rtsp_id}: {e}"
                            
#                 elif action == "delete device":
#                     for data in data_list:
#                         rtsp_id = data.get("id") #Get id and delete on that basis
#                         if rtsp_id in streams:
#                             #Delete whole data associated with it.
#                             del streams[rtsp_id]
#                             del analytics_dict[rtsp_id]
#                             del stream_metadata[rtsp_id]
                            
#                             #Stop the current stream
#                             stop_stream(rtsp_id)
#                             save_streams_to_json("streams.json",streams,analytics_dict,stream_metadata)
#                             print(f"Stream {rtsp_id} removed.")
#                             response["status"] = "success"
#                             response["message"] = f"Stream {rtsp_id} removed."
#                         else:
#                             response["status"] = "error"
#                             response["message"] = f"Stream {rtsp_id} not found."
#                             print(f"Stream {rtsp_id} not found")
                            
#                 elif action == "update device":
#                     for data in data_list:
#                         rtsp_id = data.get("id")
#                         if rtsp_id in streams:
#                             # Stop the current stream
#                             print(f"Updating stream {rtsp_id}. Stopping current stream...")
#                             stop_stream(rtsp_id)
#                             del streams[rtsp_id]
#                             del analytics_dict[rtsp_id]
#                             del stream_metadata[rtsp_id]
                            
#                             # Get updated stream details
#                             rtsp_url = data.get("url")
#                             analytics = data.get("analytics", [])
#                             name = data.get("name",f"Stream {rtsp_id}")
#                             fps = data.get("fps",3)
#                             username = data.get("username","")
#                             password = data.get("password","")
#                             # Add the updated details
#                             streams[rtsp_id] = rtsp_url
#                             analytics_dict[rtsp_id] = analytics
#                             stream_metadata[rtsp_id] = {
#                                 "name": name,
#                                 "fps" : fps,
#                                 "username" : username,
#                                 "password": password
#                             }
#                             save_streams_to_json("streams.json", streams, analytics_dict,stream_metadata)
#                             # Restart the stream
#                             try:
#                                 print(f"Starting updated stream {rtsp_id}")
#                                 start_stream(rtsp_id, rtsp_url, analytics, stream_metadata.get(rtsp_id,{}), executor)
#                                 print(f"Stream {rtsp_id} updated and restarted.")
#                                 response["status"] = "success"
#                                 response["message"] = f"Stream {rtsp_id} updated and restarted."
#                             except Exception as e:
#                                 response["status"] = "error"
#                                 response["message"] = f"Error restarting stream {rtsp_id}: {e}"
#                                 print(f"Error restarting stream {rtsp_id}: {e}")
                                
#                 elif action == "delete all":
#                     for rtsp_id in streams:
#                         stop_stream(rtsp_id)
#                         print(f"Stopping stream {rtsp_id}...")

#                     # Clear the streams, analytics_dict, and stream_metadata
#                     streams.clear()
#                     analytics_dict.clear()
#                     stream_metadata.clear()

#                     # Save the cleared to the JSON file
#                     save_streams_to_json('streams.json', streams, analytics_dict, stream_metadata)

#                     response["status"] = "success"
#                     response["message"] = "All streams have been stopped and streams.json has been cleared."
#                     print("All streams have been stopped and streams.json has been cleared.")
                    
#                 elif action == "get all":
#                     try:
#                         all_stream_data = []
#                         for rtsp_id in streams:
#                             stream_data = {
#                                 "id": rtsp_id,
#                                 "url": streams[rtsp_id],
#                                 "analytics": analytics_dict.get(rtsp_id, []),
#                                 "metadata": stream_metadata.get(rtsp_id, {})
#                             }
#                             all_stream_data.append(stream_data)
                        
#                         # Return the response
#                         response["status"] = "success"
#                         response["message"] = "All streams data retrieved successfully."
#                         response["data"] = all_stream_data
#                         print("Sent all stream data.")
                        
#                     except Exception as e:
#                         response["status"] = "error"
#                         response["message"] = f"Error retrieving all stream data: {e}"
#                         print(f"Error retrieving all stream data: {e}")
                    
                            
                                    
#         except zmq.ZMQError as e:
#             print(f"[ZMQ ERROR] {e}")
#             try:
#                 # Try sending error reply to keep REQ/REP alive
#                 rep_socket.send_json({"status": "error", "msg": str(e)})
#             except zmq.ZMQError:
#                 print("Could not send error response.")

# def main():
#     try:
#         streams, analytics_dict, stream_metadata = load_streams_from_json('streams.json')
        
#         #Sender thread (for sending detections data)
#         sender_thread = threading.Thread(target=zmq_sender_and_save_in_mongodb,daemon=True) 
#         #daemon=True , thread will automatically exit when main loop exit.
#         sender_thread.start()
                
#         with ThreadPoolExecutor(max_workers=len(streams) + 50) as executor:
#             for rtsp_id, rtsp_url in streams.items():
#                 analytics_data = analytics_dict.get(rtsp_id, [])
#                 metadata = stream_metadata.get(rtsp_id, {})
#                 start_stream(rtsp_id, rtsp_url, analytics_data, metadata, executor)
#             #Listener thread runs seperately from main execution(start streams)    
#             listener_thread = threading.Thread(target=zmq_listener,args=(streams,analytics_dict,stream_metadata,executor)) #executor passed for starting streams
#             listener_thread.start()
#             listener_thread.join() #waits for thread to finish , only exit after completion.

#     except Exception as e:
#         print(f"Error in main function: {e}")

# if __name__ == '__main__':
#     main()