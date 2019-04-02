#!/usr/bin/python3.6.3

import time
import threading
import socket
import sys

successor_peer = [-1,-1]
running= [True]
preorder_peer=[]
response_record=[0,0]

class Message():
    def __init__(self):
        self.type = ""
        self.peer = -1
        self.parameter = ""

    def get_send_message(self,type,peer,parameter):
        str =  "{0}-{1}-{2}".format(type,peer,parameter)
        return str.encode()

    def get_retuern_message(self,str):
        s = str.decode()
        l = s.split("-")
        if len(l) == 3:
            self.type = l[0]
            self.peer = int(l[1])
            self.parameter = l[2]

def analyze_udp_message(data,peer):
    m = Message()
    m.get_retuern_message(data)

    if m.type == "Ping":
        print("A ping request message was received from Peer {0}.".format(m.peer))
        if  m.peer not in preorder_peer:
            if len(preorder_peer) == 2:
                preorder_peer.clear()
            preorder_peer.append(m.peer)
        data = Message().get_send_message("Answer", peer, m.parameter)
        address = ("127.0.0.1", 50000 + m.peer)
        deliver_udp_message(data,address)
    elif m.type == "Answer":
        print("A ping response message was received from Peer {0}.".format(m.peer))
        flag = False
        bad_peer_index = -1
        if m.peer == successor_peer[0] and int(m.parameter) > response_record[0]:
            response_record[0] = int(m.parameter)
            if response_record[0] - response_record[1]>=4 and response_record[1] >0:
                flag = True
                bad_peer_index = 1
                response_record[1] = response_record[0]
        elif m.peer == successor_peer[1] and int(m.parameter) > response_record[1]:
            response_record[1] = int(m.parameter)
            if response_record[1] - response_record[0] >= 4 and response_record[0] >0:
                flag = True
                bad_peer_index = 0
                response_record[0] = response_record[1]
        if flag:
            print("Peer {0} is no longer alive.".format( successor_peer[bad_peer_index]))
            data = Message().get_send_message("Change", peer, successor_peer[bad_peer_index])
            address=("127.0.0.1",50000+successor_peer[1-bad_peer_index])
            deliver_tcp_message(data,address)



def find_file(number,peer):
    if number ==peer:
        return True
    else:
        if peer>max(preorder_peer):
            if peer >number and number > max(preorder_peer):
                return True
            else:
                return False
        else:
            if number>max(preorder_peer):
                return True
            else:
                return False

def analyze_tcp_message(data,peer):
    m = Message()
    m.get_retuern_message(data)
    if m.type == "Change":
        if successor_peer[0] == int(m.parameter):
            data =  Message().get_send_message("Successor", peer, successor_peer[1])
        else:
            data = Message().get_send_message("Successor", peer, successor_peer[0])
        address=("127.0.0.1",50000+m.peer)
        deliver_tcp_message(data,address)
        return
    if m.type == "Successor":
        if m.peer != successor_peer[0]:
            successor_peer[0] = successor_peer[1]
        successor_peer[1] = int(m.parameter)
        print("My first successor is now peer {0}.".format(successor_peer[0]))
        print("My second successor is now peer {0}.".format(successor_peer[1]))
        return
    if m.type == "Quit":
        print("Peer {0} will depart from the network.".format(m.peer))
        s_list = m.parameter.split("&")
        if m.peer == successor_peer[0]:
            successor_peer[0] = int(s_list[0])
            successor_peer[1] = int(s_list[1])
        else:
            successor_peer[1] = int(s_list[0])
        print("My first successor is now peer {0}.".format(successor_peer[0]))
        print("My second successor is now peer {0}.".format(successor_peer[1]))
        return
    if m.type == "Get File":
        number  = int(m.parameter)%256
        if find_file(number,peer) is False:
            print("File {0} is not stored here.".format(m.parameter))
            print("File request message has been forwarded to my successor.")
            data = Message().get_send_message(m.type,m.peer,m.parameter)
            address=("127.0.0.1",successor_peer[0]+50000)
            deliver_tcp_message(data,address)
        else:
            print("File {0} is here.".format(m.parameter))
            print("A response message, destined for peer {0}, has been sent.".format(m.peer))
            data = Message().get_send_message("Answer", peer, m.parameter)
            address=("127.0.0.1",m.peer+50000)
            deliver_tcp_message(data, address)
        return
    if m.type == "Answer":
        print("Received a response message from peer {0}, which has the file {1}.".format(m.peer,m.parameter))

def monitor_upd_port(port):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    address = ("127.0.0.1",port)
    udp.bind(address)
    while running[0]:
        data,_ = udp.recvfrom(1024)
        analyze_udp_message(data,port-50000)
    udp.close()

def monitor_tcp_port(port):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = ("127.0.0.1",port)
    tcp.bind(address)
    tcp.listen(5)
    while running[0]:
        client,_= tcp.accept()
        data = client.recv(1024)
        analyze_tcp_message(data,port-50000)
        client.close()
    tcp.close()

def deliver_udp_message(data,address):
    upd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    upd.sendto(data,address)
    upd.close()

def deliver_tcp_message(data,address):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect(address)
    tcp.send(data)
    tcp.close()

def ping_job(peer):
    num = 1
    while running[0]:
        for i in successor_peer:
            data = Message().get_send_message("Ping", peer,num)
            address = ("127.0.0.1", 50000 + i)
            deliver_udp_message(data,address)
        num += 1
        time.sleep(8)

if __name__ == "__main__":
    peer = int(sys.argv[1])
    successor_peer[0] = int(sys.argv[2])
    successor_peer[1] = int(sys.argv[3])

    threading.Thread(target=monitor_upd_port,args=(peer+50000,)).start()

    threading.Thread(target=monitor_tcp_port, args=(peer + 50000,)).start()

    threading.Thread(target=ping_job,args=(peer,)).start()

    while running[0]:
        str_input = input("you can input quit or request **** :\n")
        if str_input == "quit":
            running[0] = False
            for p in preorder_peer:
                data = Message().get_send_message("Quit",peer,"{0}&{1}".format(successor_peer[0],successor_peer[1]))
                address = ("127.0.0.1",50000+p)
                deliver_tcp_message(data,address)
        else:
            msgs = str_input.split(' ')
            if msgs[0] == "request" and len(msgs) == 2:
                print("File request message for {0} has been sent to my successor".format(msgs[1]))
                data = Message().get_send_message("Get File", peer, msgs[1])
                address = ("127.0.0.1", 50000 + successor_peer[0])
                deliver_tcp_message(data, address)


