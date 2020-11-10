import os

login = "ssh supriys1@taki.rs.umbc.edu"
conda_env = "/umbc/xfs1/jianwu/common/MODIS_Aggregation/supriya-code/env_stratus"

build_MODIS = "cd /home/supriys1/jianwu_common/MODIS_Aggregation/supriya-code/MODIS_Aggregation; python setup.py install; "
build_stratus_1 = "cd /home/supriys1/jianwu_common/MODIS_Aggregation/supriya-code/stratus_endpoint; python setup.py install; "
build_stratus_2 = "cd /home/supriys1/jianwu_common/MODIS_Aggregation/supriya-code/stratus_endpoint/stratus; python setup.py install rest zeromq endpoint; "

command_start_server = login + "  'source .bash_profile; source activate " + conda_env + "; "+ build_MODIS + build_stratus_1 + build_stratus_2 + " cd /home/supriys1/jianwu_common/MODIS_Aggregation/supriya-code/stratus_endpoint/stratus/stratus/handlers/endpoint/samples/MODIS_aggregation/zmq_test; python server.py &> tmp.txt' &"

os.system(command_start_server)

print('Server is running....')

os.system('python client.py')

command_stop_server = login + " 'pkill -f server.py'"

os.system(command_stop_server)

print('Server stopped.')



