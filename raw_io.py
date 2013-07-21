import socket, time

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 6379))

# get a number about the upper limit

buffer = bytearray(1024)
loop = 0
start = time.time()
LOOP = 1000000
while loop < LOOP:
    sock.send("*1\r\n$4\r\nPING\r\n")
    sock.recv_into(buffer)
    #  sock.recv(1024)  76k/s, a little slower than above, which is 78k per second
    loop += 1

t = time.time() - start
# about 78k per second on my computer
print t, 1 / (t / LOOP)
