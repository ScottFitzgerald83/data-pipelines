# Setting up the cluster (optional)
Feel free to skip this step if you have an existing cluster or don't need help setting one up. `create_cluster`  is a cluster provisioning script that reads from a file `../credentials.cfg` in the root level of the project directory. The `credentials.cfg` should resemble the following structure. Feel free to change the number of nodes or other parameters to suit your needs:

`credentials.cfg`
```
[AWS]
KEY=YOURAWSACCESSKEYHERE
SECRET=YOURSECRETACCESSKEYHERE
REGION_NAME=some-aws-region

[REDSHIFT]
CLUSTER_TYPE=multi-node
NUM_NODES=2
NODE_TYPE=dc2.large
IAM_ROLE_NAME=airflow_redshift_s3
CLUSTER_IDENTIFIER=data-pipelines
DB_NAME=sparkify
DB_USER=airflow
DB_PASSWORD=S0m3sup3r$tr0ngP@$$w0rd
PORT=5439
```

### Creating a cluster
You can create a cluster by running `./create_cluster.py create` from inside the `utils` directory. 

### Deleting a cluster
Once you're done with the cluster, `./create_cluster.py delete` will delete the cluster. Beware that this does not take a snapshot of the cluster by default.
