from flask import Flask, request, Response, make_response, send_file
from py2neo import Graph, Node, Relationship
import json
#-------initialize---------
app = Flask(__name__)
url = "http://localhost:7474/db/data/"
graph = Graph(url)
graph.delete_all()

def createNode(graph,name,uuid,wants_list,gives_list):
    for give in gives_list:
        for want in wants_list:
            match = "MATCH (n:Person) WHERE n.name = '" + name + "' AND n.uuid = '" + uuid + "' AND n.want ='" + str(want)+ "' AND n.give = '"+str(give)+"' RETURN n"
            exsiting_node = graph.evaluate(match)
            if not exsiting_node:
                # create a new node if can't find one
                new_node = Node("Person", name=name, uuid=uuid, want=want, give=give)
                graph.create(new_node)

@app.route('/data', strict_slashes=True, methods=['POST'])
def data_upload():
    data = request.get_json(silent=True)
    #-------decode json into sub lists---------
    date = data["DateTime"]
    users_list = data["Users"]
    uuid = -1
    #--------build a node----------------------
    for users in users_list:
        wants_list = users_list[users]["Wants"]
        gives_list = users_list[users]["Gives"]
        #create node
        uuid = uuid+1
        uniqueNode = createNode(graph,users,str(uuid),wants_list,gives_list)
    #--------link nodes together--------------
    graph.evaluate("MATCH (a:Person) MATCH (b:Person) WHERE a.want = b.give CREATE (b)-[:HELP]->(a)")
    #--------init a dictionary----------------
    result = {"DateTime":date, "Groups":[]}
    #---------generate loop-------------------
    for i in range(len(users_list)):
        match = "MATCH (a { uuid:'"+str(i)+"' }) MATCH (a)-[p:HELP]->(s) WITH a, s MATCH p = (s)-[r:HELP*]->(a) WITH a, nodes(p) AS ns WHERE ALL (n IN ns WHERE 1=LENGTH(FILTER(m IN ns WHERE m = n))) RETURN [a] + ns"
        final_loops = graph.evaluate(match)
        print ("this is group"+str(i+1)+":")
        #json for a single loop
        group = {"GroupID":i, "Users":{}}
        if final_loops is not None:
            for element in final_loops:
                graph.delete(element)
                print(element)
                group["Users"][str(element["name"])] = element["want"] 
            result["Groups"].append(group)
        #else:
            #print ("The is no group aviliable")
    print(result)
    result_json = json.dumps(result)
    print(result_json)
    return "upload and calculate success!"

#curl -X POST -H "Content-Type: application/json" -d @/Users/yunfeiguo/Desktop/exchange/example.json http://localhost:8081/data

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081, debug=True)