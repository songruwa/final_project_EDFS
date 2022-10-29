from flask import Flask, jsonify, request
from urllib.parse import unquote_plus

from MongoFS import MongoFS
import ordertools as SqlFS
import firebase_commands as FbFS

# configs
mongo_conn_str = "mongodb+srv://x39j1017d:aLJCQ5mMc1kulqQf@cluster0.exky2zv.mongodb.net/?retryWrites=true&w=majority"

# TODO: add pass_phrase checking to all critical apis
pass_phrase = "dsci551"

# creating a Flask app
app = Flask(__name__)
mongoClient = MongoFS(mongo_conn_str)

# hello world
@app.route('/', methods = ['GET'])
def home():
	return "hello world"


# @param: directory_path - the parent directory where the new folder will be created, should be already encoded using encodeURIComponent()
# @param: directory_name - name for the new folder to be created
# @param: db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
# example: PUT http://{domain_name}/api/v1/mkdir?directory_path=%2Ftest%2Fnewfile.txt&db=mongo
@app.route('/api/v1/mkdir', methods = ['PUT'])
def mkdir():
	args = request.args
	directory_path = unquote_plus(args.get('directory_path'))
	directory_name = args.get('directory_name')
	db = args.get('db')

	fullPath = directory_path
	if directory_path != '/':
		fullPath += "/"
	fullPath += directory_name

	res = False
	if db == 'mongo':
		res = mongoClient.mkdir(directory_path, directory_name)
	elif db == 'mysql':
		res = SqlFS.mkdir(fullPath)
	elif db == 'firebase':
		res = FbFS.mkdir(fullPath)

	if res:
		return jsonify({"res": "success"})
	return jsonify({"res": "failed"})


# @param: directory_path - the full directory for the ls command
# @param: db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
# example: GET http://{domain_name}/api/v1/ls?directory_path=%2Ftest&db=mongo
@app.route('/api/v1/ls', methods = ['GET'])
def ls():
	args = request.args
	directory_path = unquote_plus(args.get('directory_path'))
	db = args.get('db')

	if db == 'mongo':
		res = mongoClient.ls(directory_path)
		return jsonify({
			'children': res
		})
	elif db == 'mysql':
		return "NOT IMPLEMENTED"
	elif db == 'firebase':
		res = FbFS.ls(directory_path)
		return jsonify({
			'children': res
		})

	return jsonify({"res": []})


# FIXME: will exceed response size limit if file is too large. Maybe we shouldn't provide such kind of interface anyway?
# @param: file_path - the full directory for the file to be read
# @param: db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
# example: GET http://{domain_name}/api/v1/ls?file_path=%2Ftest%2Fnewfile.txt&db=mongo
@app.route('/api/v1/cat', methods = ['GET'])
def cat():
	args = request.args
	file_path = unquote_plus(args.get('file_path'))
	db = args.get('db')

	if db == 'mongo':
		res = mongoClient.cat(file_path)
		return jsonify({'content': res})
	elif db == 'mysql':
		return "NOT IMPLEMENTED"
	elif db == 'firebase':
		res = FbFS.cat(file_path)
		return jsonify({'content': res})

	return jsonify({"res": "failed"}), 400


# @param: file_path - the full directory for the file to be removed
# @param: db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
# example: PUT http://{domain_name}/api/v1/rm?file_path=%2Ftest%2Fnewfile.txt&db=mongo
@app.route('/api/v1/rm', methods = ['PUT'])
def rm():
	args = request.args
	file_path = unquote_plus(args.get('file_path'))
	db = args.get('db')

	res = False
	if db == 'mongo':
		res = mongoClient.rm(file_path)
	elif db == 'mysql':
		res = SqlFS.rm(file_path)
	elif db == 'firebase':
		res = FbFS.rm(file_path)

	if res:
		return jsonify({"res": "success"})
	return jsonify({"res": "failed"}), 400


# TODO: put is much more complicated. figure out how to implement
@app.route('/api/v1/put', methods = ['PUT'])
def put():
	args = request.args
	return "hello"


# @param: file_path - the full directory for the file
# @param: db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
# example: GET http://{domain_name}/api/v1/getPartitionLocations?file_path=%2Ftest%2Fnewfile.txt&db=mongo
@app.route('/api/v1/getPartitionLocations', methods = ['GET'])
def getPartitionLocations():
	args = request.args
	file_path = unquote_plus(args.get('file_path'))
	db = args.get('db')

	res = []
	if db == 'mongo':
		res = mongoClient.getPartitionLocations(file_path)
	elif db == 'mysql':
		res = SqlFS.getPartitionLocations(file_path)
	elif db == 'firebase':
		res = FbFS.getPartitionLocations(file_path)

	return jsonify({"partitionLocations": res})


# @param: file_path - the full directory for the file to be read
# @param: partition_num - the patition number, 0-indexed
# @param: db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
# example: GET http://{domain_name}/api/v1/getPartitionLocations?file_path=%2Ftest%2Fnewfile.txt&db=mongo
@app.route('/api/v1/readPartition', methods = ['GET'])
def readPartition():
	args = request.args
	file_path = unquote_plus(args.get('file_path'))
	partition_num = int(args.get('partition_num'))
	db = args.get('db')

	if db == 'mongo':
		res, ok = mongoClient.readPartition(file_path, partition_num)
		if not ok:
			return jsonify({"res": "failed"})
		else:
			return jsonify({'content': res})
	elif db == 'mysql':
		res = SqlFS.readPartition(file_path, partition_num)
		return jsonify({'content': res})
	elif db == 'firebase':
		res = FbFS.readPartition(file_path, partition_num)
		return jsonify({'content': res})

	return jsonify({"res": "failed"}), 400


# driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug = True)