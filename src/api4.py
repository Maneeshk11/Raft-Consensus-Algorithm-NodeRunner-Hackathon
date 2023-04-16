from flask import Flask, request, jsonify, make_response
from src.node import RaftNode
import asyncio
import sys
import threading
import json

api = Flask(__name__)

nodes = {
	"node1": {"ip": "127.0.0.1", "port": 8000},
	"node2": {"ip": "127.0.0.1", "port": 8001},
	"node3": {"ip": "127.0.0.1", "port": 8002},
	"node4": {"ip": "127.0.0.1", "port": 8003},
	"node5": {"ip": "127.0.0.1", "port": 8004},
}


# # Create and start the nodes
# tasks = []
# for node_id in nodes:
# 	node = RaftNode(node_id, nodes)
# 	tasks.apiend(asyncio.create_task(node.run()))

# # Run the nodes' event loops
# await asyncio.gather(*tasks)
node_id = "node4"
#api.run(nodes[node_id]["ip"], port=nodes[node_id]["port"])
raft_node = RaftNode(node_id, nodes)
#raft_node.state = "candidate"

@api.route("/requestVote", methods = ["GET", "POST"])
def request_vote():
	try:
		if request.method == "POST":
			raft_node.reset_timeout()
			data= {
			"term": raft_node.current_term,
			"voteGranted": True
			}
			term = request.get_json().get('term')
			if term < raft_node.current_term or raft_node.state == "candidate":
			# self.current_term = response_data['term']
			# self.state = 'follower'
				data["voteGranted"] = False
			print(f"Received Heartbeat: {data}", file=sys.stdout)
			return jsonify(data)
		#data = (raft_node.request_vote())
		#print(data, file=sys.stderr)
		#voting_data = data[0]
		#response = data[1]
	except Exception as e:
		print(9999999999999999999999)
		print(e, file=sys.stderr)
	return make_response("Success", 200)
	
@api.route("/appendEntries", methods = ["POST", "GET"])
def appendEntries():
	entries = request.get_json().get('entries')
	term_flag = entries[0]["term"]
	if request.method == "POST" and term_flag is None:
		data = {
		"term": raft_node.current_term,
		"success": True
		}
		term = request.get_json().get('term')
		if term < raft_node.current_term:
			data["success"] = False
		print(f"Received HeartBeat {data}")
		raft_node.reset_timeout()
		#raft_node.timeout = 7
		print(f"raft_node.timeout: {raft_node.timeout}")
		return jsonify(data)
	
	if term_flag is not None:
		data = request.get_json()
		response = {
		"term": raft_node.current_term,
		"ack": True
		}
		raft_node.log.append(data["entries"])
		return (response, 200)
	#data = raft_node.send_heartbeats()
	#print(data, file=sys.stderr)
	#entry = data[0]
	#response = data[1]
	return make_response("Success", 200)

@api.route("/handleLog", methods = ["POST", "GET"])
def handleLog():
	data = request.get_json()
	if data["code"] == "commit log":
		save = raft_node.log[len(raft_node.log)-1]
		json_data = json.dumps(save)
		# with open(f"{node_id}.json", "w") as f:
		# 	f.write(json_data)
		# f.close()
		existing_data = []
		try:
			with open(f"{node_id}.json", "r") as f:
				existing_data = json.load(f)
		except:
			pass
		existing_data.append(json_data)
		with open(f"{node_id}.json", "w") as f:
			json.dump(existing_data, f)

	return (f"Data saved {save}", 200)

@api.route("/client-comms", methods=["POST", "GET"])
def addComm():
	try:
		data = [request.get_json()]
		print(data)
		asyncio.run(raft_node.appendEntries(data))
	except Exception as e:
		print(f"COMMS ERROR: {e}")
	return {"code": "200"}

@api.route("/send_remaining_data", methods=["POST", "GET"])
def send_remaining_data():
	data = request.get_json()
	broken_node_id = data["node_id"]
	return {"code": "200"}

async def main():
	# Define the nodes
	await raft_node.run()
	
def run_flask():
	api.run(nodes[node_id]["ip"], port=nodes[node_id]["port"])

if __name__ == '__main__':
	flask_Thread = threading.Thread(target=run_flask)
	flask_Thread.start()

	while True:
		asyncio.run(main())