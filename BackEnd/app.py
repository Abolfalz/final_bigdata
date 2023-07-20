from flask import Flask, jsonify
from flask_cors import CORS
from kafka import KafkaConsumer
import re
import threading

app = Flask(__name__)
CORS(app)

bootstrap_servers = ['localhost:9092']
topicName = 'stream_prepro'
#consumer = KafkaConsumer(topicName, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'earliest')latest
consumer = KafkaConsumer(topicName, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'latest')

dic_hashtag_count = {}
sorted_list = []
message_last = ''

list_10_score = []
avg_score = 0


def moving_average(nums, new_num):
    if len(nums) < 10:
        nums.append(new_num)
    else:
        nums.pop(0)
        nums.append(new_num)
    return sum(nums) / len(nums)


def read_messages(consumer):
    global sorted_list
    global message_last
    global avg_score

    for message in consumer:
        message_str = message.value.decode('utf-8')
        message_str = message_str.split('*#*')
        message_f = re.sub(r'[\[\]#\'\']', '', message_str[2]).replace(" ", "").split(',')
        
        print(message_f)
        if message_f[0] != '':
            for word in message_f:
                if word in dic_hashtag_count:
                    dic_hashtag_count[word] += 1
                else:
                    dic_hashtag_count[word] = 1
        
        sorted_list = sorted(dic_hashtag_count.items(), key=lambda x: x[1],  reverse=True)
        message_last = message_str[1]
        
        avg_score = moving_average(list_10_score, float(message_str[6]))
        print(list_10_score, '\t' , avg_score)


thread = threading.Thread(target=read_messages, args=(consumer,))
thread.start()


@app.route('/inf')
def get_data():
    return jsonify(sorted_list,message_last,avg_score)


if __name__ == '__main__':
    app.run()

