#Import necessary libraries
import numpy as np
import base64
import cv2
import json
import zmq
import torch
import time 
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from ultralytics import YOLO
from PersonTracker import Tracker
import os
import sys
from pathlib import Path
import queue
from queue import Queue
from pymongo import MongoClient

#Global queue for all events
alert_queue = Queue()

gst_install_dir = Path(r"C:\gstreamer-python")
gst_bin_dir = gst_install_dir / 'bin'

sys.path.append(str(gst_install_dir / 'lib' / 'site-packages'))

os.environ['PATH'] += os.pathsep + 'C:\\gstreamer-python\\lib\\gstreamer-1.0'
os.environ['PATH']+=os.pathsep + 'C:\\GSTREAMER\\gstreamer\\build\\subprojects\\gstreamer\\libs\\gst\\helpers\\gst-plugin-scanner.exe'

os.add_dll_directory(str(gst_bin_dir))

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst

Gst.init(sys.argv)

client = MongoClient("mongodb://localhost:27017/")
db = client["Detection_Database"]
collection = db["Alerts"]

#Load config file
def load_config(config_path = 'config.json'):
    with open(config_path , 'r') as config_file:
      return json.load(config_file) #return data in config file
    
#Load classes_yolo file
with open('classes_yolo.json' , 'r') as class_file:
    data = json.load(class_file)
    CLASS_NAMES = data['CLASS_NAMES']
      
config = load_config() #Store in a variable

#Initialize ZMQ context(containers for all sockets)
context = zmq.Context()
#Choose socket from Context
zmq_socket = context.socket(zmq.PUB) #Publisher for sending data
#Takes address from config.json
zmq_socket.connect(config['zmq']['publisher']['address'])

#Initialize model
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu') #To run program on GPU
model = YOLO('yolov8s.pt') #Detection Model
model.to(device)
fall_model = YOLO('fall.pt') #Fall Model
fall_model.to(device)
abandonedmodel = YOLO('abandanSmodel.pt')
abandonedmodel.to(device)
vegetation_model = YOLO('vegetation.pt')
vegetation_model.to(device)
fire_and_smoke_model = YOLO('fireandsmoke.pt')

lock = threading.Lock() #Apply lock to avoid deadlocks
json_lock = threading.Lock()

intrusion_classes = config['detection'].get('intrusion_classes', [0]) # 0-> for person , default value if no classes present in json
max_cameras = config['detection'].get('max_cameras', 10) #10 -> default value for cameras if not present in json
crowd_threshold = config['detection'].get('crowd_threshold')
loitering_threshold = config['detection'].get('loitering_threshold')
fall_threshold = config['detection'].get('fall_threshold')
fire_threshold = config['detection'].get('fire_threshold')

#Initialize all dictionaries and variables
pipelines = {} #Dictionary to save pipelines based on id for flushing in case of deletion.
stream_flags = {} #Marks the stream as active , keeps track of whether stream is running.
streams_thread= {} #Keeps track of all threads
entry_time = {}
abandoned_frame_counter = {}
movement_history = {}

#Load data from json file
def load_streams_from_json(file_path):
    try:
      with open(file_path,'r') as file:
          data = json.load(file)
          
          #Create empty dictionaries and extract data from json file and put them seperately
          streams = {}
          analytics_dict = {}
          stream_metadata = {}
          
          for stream in data['streams']:
              rtsp_id = stream['id']
              streams[rtsp_id] = stream['url']
              analytics_dict[rtsp_id] = stream.get('analytics',[])
              
              #Save additional metadata
              stream_metadata[rtsp_id]={
                  "name":stream.get('name', f'Stream{rtsp_id}'),
                  "fps":stream.get('fps', 2),
                  "username":stream.get('username',''),
                  "password":stream.get('password','')
              }
          return streams, analytics_dict, stream_metadata
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return {}, {}, {}
    
#Start stream using threadpoolexecutor
def start_stream(rtsp_id,rtsp_url,analytics_dict,metadata,executor):
    #Start the stream in a seperate thread
    print(f"Starting thread for Stream ID: {rtsp_id} and {rtsp_url}", flush=True)
    try:
        stream_flags[rtsp_id] = True  # Set the flag to True to allow processing
        stream_thread = executor.submit(process_stream, rtsp_id, rtsp_url, analytics_dict, metadata)
        streams_thread[rtsp_id] = stream_thread
    except Exception as e:
        print(f"Failed to start Stream {rtsp_id}: {e}")
        print('Retrying')
        
def stop_stream(rtsp_id):
    #Stop the thread for specific rtsp_id if running
    print(f"Stopping thread for {rtsp_id}")
    stream_flags[rtsp_id] = False
    if rtsp_id in streams_thread:
        future = streams_thread[rtsp_id]
        future.result()
        del streams_thread[rtsp_id]
        #Flush the pipeline
        pipeline = pipelines[rtsp_id]
        pipeline.set_state(Gst.State.NULL)
        del pipelines[rtsp_id] 
        print("Pipeline flushed and thread stopped")
        # cv2.destroyWindow(f"Stream {rtsp_id}")
        # print(f"OpenCV window for Stream {rtsp_id} closed.")
        
def is_inside_roi(center_points , roi_points):
    if len(roi_points)>2:
        result = cv2.pointPolygonTest(roi_points,center_points,False)
        return result>=0
    
def zmq_sender_and_save_in_mongodb():
    socket = context.socket(zmq.PUB)
    try:
        socket.connect("tcp://127.0.0.1:5555")
    except zmq.ZMQError as e:
        print(f"ZMQ Connection Error: {e}")
        return
    try:
        while True:
            try:
                queue_time, payload = alert_queue.get()
                data_sent_to_mongodb = {
                    "ID" : payload.get("ID"),
                    "Event" : payload.get("Event"),
                    "Timestamp" : payload.get("Timestamp")
                }
                get_time = time.time()
                #Calculate delay
                queue_delay_time = get_time - queue_time
                # print(f'Queue delay time: {queue_delay_time}')
                # print(f"Retrieved from alert_queue: {payload}")
                json_data = json.dumps(payload)
                socket.send_string(json_data)
                collection.insert_one(data_sent_to_mongodb)
                # print(f"Sent to ZMQ: {json_data}") 
                alert_queue.task_done() #Ensures item is processed and memory is released.
                del payload #Memory clean explicitly
            except queue.Empty:
                pass #No data to send , retry loop
    except Exception as e:
        print(f"Error in zmq : {e}")
        
def zmq_receive_loop():
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics

    subscriber_addresses = config["zmq"]["subscribers"]["addresses"]
    
    for address in subscriber_addresses:
        try:
            print(f"Connecting subscriber to {address}")
            socket.connect(address)
        except zmq.ZMQError as e:
            print(f"ZMQ Connection Error while connecting to {address}: {e}")

    print("Receiving messages...")
    while True:
        message = socket.recv_string()
        print(f"Received: {message}")

def save_frame_and_send_intrusion_alert(rtsp_id,frame):
        folder_path=f"./Intrusion/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f"{rtsp_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['intrusion']
        payload = {
            "ID":rtsp_id,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "FrameData":img_base64,
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
    
def save_frame_and_send_loitering_alert(rtsp_id,frame):
        folder_path=f"./Loitering/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f"{rtsp_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['loitering']
        payload = {
            "ID":rtsp_id,
            "FrameData":img_base64,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
        
def save_frame_and_send_crowd_alert(rtsp_id,frame):
        folder_path=f"./Crowd/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f"{rtsp_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['crowd_alert']
        payload = {
            "ID":rtsp_id,
            "FrameData":img_base64,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
        
def save_frame_and_send_fall_alert(rtsp_id,frame):
        folder_path=f"./Fall/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f'{rtsp_id}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.jpg')
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['fall']
        payload = {
            "ID":rtsp_id,
            "FrameData":img_base64,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
        
def save_frame_and_send_abandon_alert(rtsp_id,frame):
        folder_path=f"./Abandon/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f'{rtsp_id}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.jpg')
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['object_abandoned']
        payload = {
            "ID":rtsp_id,
            "FrameData":img_base64,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
        
def save_frame_and_send_vegetation_alert(rtsp_id,frame):
        folder_path=f"./Vegetation/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f'{rtsp_id}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.jpg')
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['vegetation']
        payload = {
            "ID":rtsp_id,
            "FrameData":img_base64,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
        
def save_frame_and_send_fire_and_smoke_alert(rtsp_id,frame):
        folder_path=f"./FireandSmoke/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(folder_path,exist_ok=True)
        filename = os.path.join(folder_path, f'{rtsp_id}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.jpg')
        cv2.imwrite(filename, frame)
        _ , buffer = cv2.imencode('.jpg',frame)
        img_base64 = base64.b64encode(buffer).decode('utf-8')
        event_config = config['events']['fire_and_smoke']
        payload = {
            "ID":rtsp_id,
            "FrameData":img_base64,
            "Event":event_config.get('event_type'),
            "Timestamp": datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            "Detection":True
        }
        #Put payload in the queue
        queue_time = time.time()
        alert_queue.put((queue_time , payload))
                
def process_stream(rtsp_id,rtsp_url,analytics_dict,stream_metadata):
    print(f"process_stream() started for Stream ID: {rtsp_id}",flush=True)
    
    intrusion_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "intrusion"), []), dtype=np.int32
    ).reshape((-1, 1, 2))

    crowd_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "crowd"), []), dtype=np.int32
    ).reshape((-1, 1, 2))

    loitering_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "loitering"), []), dtype=np.int32
    ).reshape((-1, 1, 2))
    
    fall_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "fall"), []), dtype=np.int32
    ).reshape((-1, 1, 2))
    
    abandon_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "object_abandoned"), []), dtype=np.int32
    ).reshape((-1, 1, 2))
    
    vegetation_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "vegetation"), []), dtype=np.int32
    ).reshape((-1, 1, 2))
    
    fire_and_smoke_roi = np.array(
        next((analytic["roi"] for analytic in analytics_dict if analytic["type"] == "vegetation"), []), dtype=np.int32
    ).reshape((-1, 1, 2))
    
    last_save_intrusion_time = 0  #Track last save intrusion time
    last_save_loitering_time = 0 #Track last save loitering time
    crowd_last_save_time = 0 #Track last save crowd time
    fall_counter=0
    fire_counter = 0
    fall_save_time = 0
    last_save_abandon_time = 0
    last_save_vegetation_time = 0
    fire_and_smoke_time = 0
    
    fps = stream_metadata['fps']
    # Define the GStreamer pipeline
    #rtph264depay ! h264parse ! avdec_h264 !
    gst_pipeline = (
        f"filesrc location={rtsp_url} ! "
        f"decodebin ! videoconvert ! videoscale ! videorate ! "
        f"video/x-raw, width=640, height=640, format=BGR, framerate={fps}/1 ! "
        f"appsink name=sink emit-signals=True"
    )
    print(f"GStreamer Pipeline for Stream {rtsp_id}: {gst_pipeline}")
    try:
        # Create and start the GStreamer pipeline
        pipeline = Gst.parse_launch(gst_pipeline)
        appsink = pipeline.get_by_name("sink")  # Get the appsink element

        if not appsink:
            print("Error: Appsink element not found in the pipeline!")
            return

        pipeline.set_state(Gst.State.PLAYING)  # Start the pipeline
        # Track the pipeline
        pipelines[rtsp_id] = pipeline  # Track the pipeline by rtsp_id
    
        # Run the stream processing loop while flag is True
        while stream_flags[rtsp_id]:
            # Pull frame sample
            sample = appsink.emit("pull-sample")
            if sample is None:
               continue

            buf = sample.get_buffer() # Get frames of the video through rtsp url
            caps = sample.get_caps()  # Capabilities including formats , resolution 
            height = caps.get_structure(0).get_value("height") #640 from cap
            width = caps.get_structure(0).get_value("width") #640 from cap
        
            success, map_info = buf.map(Gst.MapFlags.READ)
            if not success:
                continue  # Skip frame if mapping fails

            # Convert buffer to numpy array
            frame = np.ndarray(
                shape=(height, width, 3),
                dtype=np.uint8,
                buffer=map_info.data
            )
            
        
            frame = cv2.resize(frame,(640,640))
            results = model(frame,classes=intrusion_classes,iou=0.25,conf=0.5,verbose=False)  #verbose produces cleaner outputs, suppress the logs
                  
            detected_objects = []  
            person_bboxes = []
            #Initialize tracker
            person_tracker = Tracker()
            abandoned_tracker = Tracker()
            
            for result in results:
                for box in result.boxes:
                    x1,y1,x2,y2 = map(int,box.xyxy[0])
                    class_id = int(box.cls[0]) #class id
                    class_name = CLASS_NAMES.get(str(class_id),"Unknown")
                    detected_objects.append({"bbox":[x1,y1,x2,y2] , "class_id":class_id , "class_name":class_name})
                    if class_id == 0:
                        person_bboxes.append([x1,y1,x2,y2])
                        
                
        #     #INTRUSION DETECTION
        #     #Check detection is inside roi
            # if "intrusion" in [analytic["type"] for analytic in analytics_dict]:
            #     detection_in_roi = False
            #     for obj in detected_objects:
            #         x1, y1, x2, y2 = obj["bbox"]
            #         class_id = obj["class_id"]
            #         class_name = obj["class_name"]
            #         centers = ((x1 + x2) // 2, (y1 + y2) // 2)
            #         if is_inside_roi(centers,intrusion_roi) and class_id in intrusion_classes:
            #             detection_in_roi = True
            #             cv2.rectangle(frame, (x1, y1), (x2, y2), (255, 0, 0), 2)
            #             text = f"{class_name}"
            #             (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
            #             cv2.rectangle(frame, (x1, y1 - text_h - 10), (x1 + text_w, y1), (0, 0, 0), -1)
            #             cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
            #     current_time = time.time()
            #     if detection_in_roi and (current_time - last_save_intrusion_time >= 5):
            #         save_frame_and_send_intrusion_alert(rtsp_id,frame)
            #         last_save_intrusion_time = current_time
                            
        ##Loitering Detection  
            if "loitering" in [analytic["type"] for analytic in analytics_dict]:
                # Assign IDs using person_tracker
                # print(person_bboxes)
                objects_bbs_ids, _ = person_tracker.update(person_bboxes)
                # Track time and alert for intrusion and loitering
                # print(objects_bbs_ids)
                current_time = time.time()
                for obj in objects_bbs_ids:
                    x,y,w,h,person_id = obj
                    cx,cy = x+w//2 , y+h//2 # Calculate center of bounding box
                    print(f".......{(cx,cy)}")
                    if person_id not in movement_history:
                        movement_history[person_id] = [] #Initialise
                    movement_history[person_id].append((cx,cy)) #Centers of bounding box    

                    cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)  # Bounding box
                    text = f"person ID: {person_id}"  # Display person ID
                    cv2.putText(frame, text, (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
                    
                    if is_inside_roi((cx,cy),loitering_roi):
                     # Loitering alert for new entry into ROI
                        if person_id not in entry_time:
                            entry_time[person_id] = current_time
                            #print(f"Person {person_id} entered ROI at {entry_time[person_id]}")
                        time_in_roi = current_time - entry_time[person_id]
                        #print(f"Person {person_id} Time in ROI: {time_in_roi}")
                        if time_in_roi >= loitering_threshold:
                            #print('Loitering detected')
                            current_time = time.time()
                            
                            # Check if the last loitering alert was sent more than loitering_alert_interval ago
                            if current_time - last_save_loitering_time >= 5:
                                
                                cv2.rectangle(frame, (x, y), (x +w, y + h), (255, 255, 255), 2)
                                movement = movement_history.get(person_id,[])
                                if len(movement)>=2 : #Lines will be drawn between two points
                                    for i in range(1,len(movement)):
                                        pt1 = movement[i - 1]
                                        pt2 = movement[i]
                                        cv2.line(frame, pt1, pt2, (0, 0, 255), 2)
                                        print(pt1,pt2)
                                        
                                for (mx,my) in movement:
                                    cv2.circle(frame,(mx,my),radius=4,color=(255,255,0),thickness=-1)
                                save_frame_and_send_loitering_alert(rtsp_id, frame)
                                last_save_loitering_time = current_time
                                print(movement_history)
                    else:
                        # Remove tracking if person leaves ROI
                        if person_id in entry_time:
                            del entry_time[person_id]
                        if person_id in movement_history:
                            del movement_history[person_id]
                        print(movement_history)
            
        #      #CROWD DETECTION
        #     if "crowd" in [analytic["type"] for analytic in analytics_dict]:
        #         persons_in_roi = 0 #Initialise persons 
        #         for x1, y1, x2, y2 in person_bboxes:
        #             center = (int((x1 + x2) / 2), int((y1 + y2) / 2))
        #             if is_inside_roi(center,crowd_roi):
        #                 persons_in_roi += 1  #Increase counter
        #                 #print('Persons in ROI:',persons_in_roi)
        #                 if persons_in_roi > crowd_threshold:
        #                     current_time = time.time()
        #                     if current_time - crowd_last_save_time >=5 :
        #                         save_frame_and_send_crowd_alert(rtsp_id,frame),
        #                         crowd_last_save_time = current_time
                                    
        #   #FALL DETECTION
            # if "fall" in [analytic["type"] for analytic in analytics_dict]:
            #     fall_results = fall_model.predict(frame,iou=0.25,conf=0.5,verbose = False)
            #     fall_detected = False #Flag to check fall is detected
            #     for result in fall_results:
            #         for box in result.boxes:
            #             x1,y1,x2,y2 = map(int,box.xyxy[0])
            #             cx , cy = x1 + x2 //2 , y1 + y2 //2
            #             cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
            #             text = "Fall Detected"
            #             (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
            #             cv2.rectangle(frame, (x1, y1 - text_h - 10), (x1 + text_w, y1), (0, 0, 0), -1)
            #             cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
            #             #print(f"Fall detected at: ({cx}, {cy}), Box: ({x1}, {y1}, {x2}, {y2})") 
            #             if is_inside_roi((cx,cy),fall_roi):
            #                 fall_detected = True
            #                 fall_counter+=1  #Increment the counter for consecutive fall detection
            #                 #If fall detected for consecutive fall_threshold frames
            #                 #print(fall_counter)
            #                 if fall_counter >= fall_threshold:
            #                     current_time = time.time()
                                
            #                     #print(f"Fall detected and bounding box drawn at: ({x1}, {y1}, {x2}, {y2})") 
            #                     if current_time - fall_save_time >= 5:
            #                         save_frame_and_send_fall_alert(rtsp_id,frame)
            #                         fall_save_time = current_time
            #                         #Reset frame counter
            #                         fall_counter = 0 #After fall threshold 
            #                 else:
            #                     #Reset frame counter if no fall is detected (fall detected in consecutive 2 frames , after that no fall is detected , so make counter as 0).
            #                     fall_counter = 0
            
        #     #Abandoned object                
        #     if "object_abandoned" in [analytic["type"] for analytic in analytics_dict]:
        #             abandon_results = abandonedmodel.predict(frame, conf=0.2, iou=0.25)
        #                 # Append all detected bounding boxes (x , y , w, h)
        #             abandoned_bboxes = [
        #                     [int(box.xyxy[0][0]), int(box.xyxy[0][1]), int(box.xyxy[0][2]) - int(box.xyxy[0][0]), int(box.xyxy[0][3]) - int(box.xyxy[0][1])]
        #                     for box in abandon_results[0].boxes
        #                 ]
                     
        #             tracked_objects, _ = abandoned_tracker.update(abandoned_bboxes)
        #             for tracked_obj in tracked_objects:
        #                 x, y, w, h, obj_id = tracked_obj
        #                 point_check = (x + w // 2, y + h // 2)

        #                 #Track for how long the objects stays (by checking on frames)
        #                 if obj_id not in abandoned_frame_counter:
        #                     abandoned_frame_counter[obj_id] = 1
        #                 else:
        #                     abandoned_frame_counter[obj_id] += 1
                            
        #                 if abandoned_frame_counter[obj_id] >= 10:
        #                     if is_inside_roi(point_check,abandon_roi):
        #                     #Check horizontal and vertical overlaps up,down,right,left (check for personbbox and objectbbox)
        #                         is_intersecting_person = any(
        #                             (x < x2 and x + w > x1) and (y < y2 and y + h > y1)
        #                                 for x1, y1, x2, y2 in person_bboxes
        #                             )
        #                         if not is_intersecting_person:
        #                             current_time = time.time()
        #                             if current_time - last_save_abandon_time >=5 :
        #                                 # Highlight the abandoned object
        #                                     cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 0, 255), 2)
        #                                     text = "Abandoned Object"
        #                                     (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
        #                                     cv2.rectangle(frame, (x, y - text_h - 10), (x + text_w, y), (0, 0, 0), -1)
        #                                     cv2.putText(frame, text, (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
        #                                     save_frame_and_send_abandon_alert(rtsp_id,frame)
        #                                     last_save_abandon_time = current_time
            
            # #Vegetation model
            # if "vegetation" in [analytic["type"] for analytic in analytics_dict]:
            #     vegetation_results = vegetation_model.predict(frame,iou=0.25,conf=0.5) 
            #     detection_in_roi = False    
            #     for result in vegetation_results:
            #         for box in result.boxes:
            #             x1, y1, x2, y2 = map(int, box.xyxy[0])
            #             conf = float(box.conf[0])
            #             cls = int(box.cls[0])
            #             class_name = vegetation_model.names[cls]
            #             centers = ((x1 + x2) // 2, (y1 + y2) // 2)
            #             if is_inside_roi(centers,vegetation_roi):
            #                 detection_in_roi = True  # Flag that we have a detection in ROI
            #                 cv2.rectangle(frame, (x1, y1), (x2, y2), (255, 0, 0), 2)
            #                 text = f"{class_name}"
            #                 (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
            #                 cv2.rectangle(frame, (x1, y1 - text_h - 10), (x1 + text_w, y1), (0, 0, 0), -1)
            #                 cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
            #     # After processing all boxes, save frame if detections are in ROI
            #     current_time = time.time()
            #     if detection_in_roi and (current_time - last_save_vegetation_time >= 5):
            #         save_frame_and_send_vegetation_alert(rtsp_id, frame)
            #         last_save_vegetation_time = current_time     
             
            # if "fire_and_smoke" in [analytic["type"] for analytic in analytics_dict]:
            #        fire_and_smoke_results = fire_and_smoke_model.predict(frame,iou=0.25)
            #        detection_in_roi = False
            #        for result in fire_and_smoke_results:
            #            for box in result.boxes:
            #                x1, y1, x2, y2 = map(int, box.xyxy[0])
            #                conf = float(box.conf[0])
            #                cls = int(box.cls[0])
            #                class_name = fire_and_smoke_model.names[cls]
            #                centers = ((x1 + x2) // 2, (y1 + y2) // 2)
            #                if is_inside_roi(centers,fire_and_smoke_roi):
            #                 detection_in_roi = True
            #                 #fire_counter += 1
            #                 cv2.rectangle(frame, (x1, y1), (x2, y2), (255, 0, 0), 2)
            #                 text = f"{class_name}"
            #                 (text_w, text_h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
            #                 cv2.rectangle(frame, (x1, y1 - text_h - 10), (x1 + text_w, y1), (0, 0, 0), -1)
            #                 cv2.putText(frame, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
            #                 # if fire_counter >= fire_threshold:
            #        current_time = time.time()
            #      if detection_in_roi and (current_time - fire_and_smoke_time >= 5):
        #             save_frame_and_send_fire_and_smoke_alert(rtsp_id,frame)
        #             fire_and_smoke_time = current_time
                     #fire_counter = 0
                #  else:
                #     fire_counter = 0
                        
            #cv2.polylines(frame, [loitering_roi], isClosed=True, color=(0,0,255), thickness=2)
            #cv2.polylines(frame, [crowd_roi], isClosed=True, color=(0,255,255), thickness=2)
            #cv2.polylines(frame, [intrusion_roi], isClosed=True, color=(0,255,255), thickness=2)
            #cv2.polylines(frame, [fall_roi], isClosed=True, color=(0,255,255), thickness=2)  
            
            
                    
            if frame is not None:
                window_name = f"Stream {rtsp_id}"
                cv2.imshow(window_name, frame)
            else:
                print(f"No valid frame for Stream {rtsp_id}") 

            # Exit on 'ESC' key
            if cv2.waitKey(1) & 0xFF == 27:
                break
        cv2.destroyAllWindows()    
            
        buf.unmap(map_info)

    except Exception as e:
        print(f"Pipeline creation failed: {e}") 

    finally:
        # Ensure the pipeline is stopped properly when exiting
        pipeline.set_state(Gst.State.NULL)
        
def save_streams_to_json(file_path,streams,analytics_dict,stream_metadata):
    try:
        with json_lock:
            data = {"streams": []}
            for rtsp_id, rtsp_url in streams.items():
                stream_data = {
                    "id": rtsp_id,
                    "url": rtsp_url,
                    "analytics": analytics_dict.get(rtsp_id, []),
                    "name": stream_metadata.get(rtsp_id, {}).get("name", f"Stream {rtsp_id}"),
                    "fps": stream_metadata.get(rtsp_id, {}).get("fps", 3),
                    "username": stream_metadata.get(rtsp_id, {}).get("username", ""),
                    "password": stream_metadata.get(rtsp_id, {}).get("password", "")
                }
                data["streams"].append(stream_data)
        
                 # Save data to file
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"Data successfully saved to {file_path}")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}. Please check the file path.")
    except Exception as e:
        print(f"Failed to save JSON: {e}")

                
def zmq_listener(streams,analytics_dict,stream_metadata,executor):
    rep_socket = context.socket(zmq.REP)
    try:
        rep_socket.bind("tcp://*:5556")  
       
    except zmq.ZMQError as e:
        print(f"Failed to bind socket: {e}")
        return  # Exit if binding fails
    while True:
        print('Starting listener thread')
        try:
            response = {"status" : "error" , "message" : "server error"}
            
            #Receive and parse the message
            message = rep_socket.recv_json()
            print(f"Received message:{message}")
            rep_socket.send_json({"status": "ok"})
            
            action = message.get("action")
            data_list = message.get("data",[])
            
            with lock:
                if action == "add device":
                    for data in data_list:
                        rtsp_id = data.get("id")
                        rtsp_url = data.get("url")
                        analytics = data.get("analytics",[])
                        name = data.get("name",f"Stream {rtsp_id}")
                        fps = data.get("fps",3)
                        username = data.get("username","")
                        password = data.get("password","")
                        if rtsp_id in streams:
                            print(f"Stream with ID {rtsp_id} already exists.")
                            response["status"] = "error"
                            response["message"] = f"Stream with ID {rtsp_id} already exists."
                        else:
                            streams[rtsp_id] = rtsp_url
                            analytics_dict[rtsp_id] = analytics
                            stream_metadata[rtsp_id] = {
                                "name":name,
                                "fps":fps,
                                "username":username,
                                "password":password
                            }
                            stream_flags[rtsp_id] = True
                        try:
                            start_stream(rtsp_id, streams[rtsp_id], analytics, stream_metadata.get(rtsp_id,{}), executor)
                            save_streams_to_json("streams.json",streams,analytics_dict,stream_metadata)
                            print(f"Stream {rtsp_id} added with analytics: {analytics}")
                            response["status"] = "success"
                            response["message"] = f"Stream {rtsp_id} started."
                        except Exception as e:
                            response["status"] = "error"
                            response["message"] = f"Error while submitting stream {rtsp_id}: {e}"
                            
                elif action == "delete device":
                    for data in data_list:
                        rtsp_id = data.get("id") #Get id and delete on that basis
                        if rtsp_id in streams:
                            #Delete whole data associated with it.
                            del streams[rtsp_id]
                            del analytics_dict[rtsp_id]
                            del stream_metadata[rtsp_id]
                            
                            #Stop the current stream
                            stop_stream(rtsp_id)
                            save_streams_to_json("streams.json",streams,analytics_dict,stream_metadata)
                            print(f"Stream {rtsp_id} removed.")
                            response["status"] = "success"
                            response["message"] = f"Stream {rtsp_id} removed."
                        else:
                            response["status"] = "error"
                            response["message"] = f"Stream {rtsp_id} not found."
                            print(f"Stream {rtsp_id} not found")
                            
                elif action == "update device":
                    for data in data_list:
                        rtsp_id = data.get("id")
                        if rtsp_id in streams:
                            # Stop the current stream
                            print(f"Updating stream {rtsp_id}. Stopping current stream...")
                            stop_stream(rtsp_id)
                            del streams[rtsp_id]
                            del analytics_dict[rtsp_id]
                            del stream_metadata[rtsp_id]
                            
                            # Get updated stream details
                            rtsp_url = data.get("url")
                            analytics = data.get("analytics", [])
                            name = data.get("name",f"Stream {rtsp_id}")
                            fps = data.get("fps",3)
                            username = data.get("username","")
                            password = data.get("password","")
                            # Add the updated details
                            streams[rtsp_id] = rtsp_url
                            analytics_dict[rtsp_id] = analytics
                            stream_metadata[rtsp_id] = {
                                "name": name,
                                "fps" : fps,
                                "username" : username,
                                "password": password
                            }
                            save_streams_to_json("streams.json", streams, analytics_dict,stream_metadata)
                            # Restart the stream
                            try:
                                print(f"Starting updated stream {rtsp_id}")
                                start_stream(rtsp_id, rtsp_url, analytics, stream_metadata.get(rtsp_id,{}), executor)
                                print(f"Stream {rtsp_id} updated and restarted.")
                                response["status"] = "success"
                                response["message"] = f"Stream {rtsp_id} updated and restarted."
                            except Exception as e:
                                response["status"] = "error"
                                response["message"] = f"Error restarting stream {rtsp_id}: {e}"
                                print(f"Error restarting stream {rtsp_id}: {e}")
                                
                elif action == "delete all":
                    for rtsp_id in streams:
                        stop_stream(rtsp_id)
                        print(f"Stopping stream {rtsp_id}...")

                    # Clear the streams, analytics_dict, and stream_metadata
                    streams.clear()
                    analytics_dict.clear()
                    stream_metadata.clear()

                    # Save the cleared to the JSON file
                    save_streams_to_json('streams.json', streams, analytics_dict, stream_metadata)

                    response["status"] = "success"
                    response["message"] = "All streams have been stopped and streams.json has been cleared."
                    print("All streams have been stopped and streams.json has been cleared.")
                    
                elif action == "get all":
                    try:
                        all_stream_data = []
                        for rtsp_id in streams:
                            stream_data = {
                                "id": rtsp_id,
                                "url": streams[rtsp_id],
                                "analytics": analytics_dict.get(rtsp_id, []),
                                "metadata": stream_metadata.get(rtsp_id, {})
                            }
                            all_stream_data.append(stream_data)
                        
                        # Return the response
                        response["status"] = "success"
                        response["message"] = "All streams data retrieved successfully."
                        response["data"] = all_stream_data
                        print("Sent all stream data.")
                        
                    except Exception as e:
                        response["status"] = "error"
                        response["message"] = f"Error retrieving all stream data: {e}"
                        print(f"Error retrieving all stream data: {e}")
                        
                elif action == "delete analytic":
                    for data in data_list:
                        rtsp_id = data.get("id")
                        analytic_to_delete = data.get("analytic_type")
                        
                        if rtsp_id in streams:
                            #Get existing analytics for this rtsp_id
                            existing_analytics = analytics_dict.get(rtsp_id,[])
                            
                            #Find the analytic with specified type and remove it.
                            analytic_found = False
                            for analytic in existing_analytics:
                                if analytic.get("type")==analytic_to_delete:
                                    existing_analytics.remove(analytic)
                                    analytic_found = True
                                    print(f"Deleted analytic of type '{analytic_to_delete}' from stream {rtsp_id}.")
                                    break
                                
                            if analytic_found:
                                #Stop that stream associated with that analytic
                                stop_stream(rtsp_id)
                                
                                #Update the analytics_dict with updated analytics
                                analytics_dict[rtsp_id] = existing_analytics
                                
                                # Save the updated streams and analytics to JSON
                                save_streams_to_json('streams.json', streams, analytics_dict, stream_metadata)
                                
                                # Restart the stream with updated analytics (without the deleted analytic)
                                try:
                                    print(f"Restarting stream {rtsp_id} with updated analytics...")
                                    start_stream(rtsp_id, streams[rtsp_id], analytics_dict[rtsp_id], stream_metadata.get(rtsp_id, {}), executor)
                                    print(f"Stream {rtsp_id} restarted with updated analytics.")
                                    response["status"] = "success"
                                    response["message"] = f"Stream {rtsp_id} updated by removing '{analytic_to_delete}' analytic and restarted."
                                except Exception as e:
                                    response["status"] = "error"
                                    response["message"] = f"Error restarting stream {rtsp_id}: {e}"
                                    print(f"Error restarting stream {rtsp_id}: {e}")
                            else:
                                response["status"] = "error"
                                response["message"] = f"Analytic of type '{analytic_to_delete}' not found for stream {rtsp_id}. Cannot delete."
                                print(f"Analytic of type '{analytic_to_delete}' not found for stream {rtsp_id}. Cannot delete.")
                        else:
                            response["status"] = "error"
                            response["message"] = f"Stream {rtsp_id} not found. Cannot delete analytic."
                            print(f"Stream {rtsp_id} not found. Cannot delete analytic.")
                    
                            
                                    
        except zmq.ZMQError as e:
            print(f"[ZMQ ERROR] {e}")
            try:
                # Try sending error reply to keep REQ/REP alive
                rep_socket.send_json({"status": "error", "msg": str(e)})
            except zmq.ZMQError:
                print("Could not send error response.")

def main():
    try:
        streams, analytics_dict, stream_metadata = load_streams_from_json('streams.json')
        
        #Sender thread (for sending detections data)
        sender_thread = threading.Thread(target=zmq_sender_and_save_in_mongodb,daemon=True) 
        #daemon=True , thread will automatically exit when main loop exit.
        sender_thread.start()
                
        with ThreadPoolExecutor(max_workers=len(streams) + 50) as executor:
            for rtsp_id, rtsp_url in streams.items():
                analytics_data = analytics_dict.get(rtsp_id, [])
                metadata = stream_metadata.get(rtsp_id, {})
                start_stream(rtsp_id, rtsp_url, analytics_data, metadata, executor)
            #Listener thread runs seperately from main execution(start streams)    
            listener_thread = threading.Thread(target=zmq_listener,args=(streams,analytics_dict,stream_metadata,executor)) #executor passed for starting streams
            listener_thread.start()
            listener_thread.join() #waits for thread to finish , only exit after completion.

    except Exception as e:
        print(f"Error in main function: {e}")

if __name__ == '__main__':
    main()