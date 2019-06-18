# robust-journey-planning

## How to launch the visualization
1. You may have to first setup a SOCKS Proxy in your browser settings. (IP: 127.0.0.1, port: 8080)
   For more details on this see [this link](https://askubuntu.com/questions/414930/access-webpage-through-ssh)
2. On your machine, SSH into the cluster with 'ssh -L 5000:localhost:5000 [USERNAME]@iccluster042.iccluster.epfl.ch'
3. Once in the cluster, go to 'visualization/app' and run 'export FLASK_APP=__init__.py'
4. Launch the server with 'flask run'
5. Once the SparkSession has been created an ip address will be displayed. Copy paste it into your browser to access the visualization that is now running on the cluster.
