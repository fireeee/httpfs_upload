#!/usr/bin/python

def make_httpfs_url(host, user, hdfs_path, op, port=14000):

    url = 'http://' + user + '@' + host + ':' + str(port) + '/webhdfs/v1'
    url += hdfs_path + '?user.name=' + user + '&op=' + op

    return url

def put(host, user, hdfs_path, filename, port=14000, perms=775):

    # Get the file name without base path.
    filename_short = filename.split('/')[-1]
    # Form the URL.
    url = make_httpfs_url(
        host=host,
        user=user,
        hdfs_path=hdfs_path + '/' + filename_short,
        op='CREATE&data=true&overwrite=true&permission=' + str(perms),
        port=port
    )
    headers = {
        'Content-Type':'application/octet-stream'
    }
    #files = {'file': open(filename,'rb')}

    resp = requests.put(url, data=open(filename,'rb'), headers=headers)
    if resp.status_code != 200:
        resp.raise_for_status()
