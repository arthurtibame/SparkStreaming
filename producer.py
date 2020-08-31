#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka import Producer
import sys
import pandas as pd
import time
from datetime import datetime,timedelta



def error_cb(err):
    print('Error: %s' % err)



if __name__ == '__main__':

    props = {

        'bootstrap.servers': '10.120.26.247:9092',
        'error_cb': error_cb
    }

    producer = Producer(props)

    topicName = 'testing'
    msgCounter = 0
    try:
        while True:
            news_df = pd.read_csv(r'./gpsData.csv', error_bad_lines=False)
            gps_data = news_df.values.tolist()
            for data in gps_data:
                gps_time = data[1]
                gps_time = datetime.strptime(gps_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                tw_time = timedelta(hours=8) + gps_time
                lat = float(data[2])
                lon = float(data[3])
                location = [lat, lon]
                alt = data[4]
                speed = data[5]
                climb = data[6]
                track = data[7]
                eps = data[8]
                epx = data[9]
                epv = data[10]
                ept = data[11]
                fixtype = data[12]

                loc_dict = {
                    # "time_stamp" : time_stamp,
                    "gps_time": tw_time,
                    "location": location,
                    "alt": alt,
                    "speed": speed,
                    "climb": climb,
                    "track": track,
                    "eps": eps,
                    "epx": epx,
                    "epv": epv,
                    "ept": ept,
                    "fixtype": fixtype
                }
                c = str(loc_dict)

                # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
                producer.produce(topicName, value=bytes(c,encoding="utf8"))

                # producer.flush()

                print('Send ' + str(msgCounter) + ' messages to Kafka' + c)
                time.sleep(1)
                msgCounter += 1
    except BufferError as e:

        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)

    producer.flush()

