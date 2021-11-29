
from danglinglink import DanglingNodeJob
from pagerank import PageRankJob
from prdiff import DiffPagerank
from graphsize import CountGraphNodesJob
from initPageRank import DistributeInitialPageRankJob
from linkgraph import MRCalculateLinkGraph
from datetime import datetime


#################################################################################################################

#runner_type = 'hadoop'
#output_dir = 'hdfs://x18201148-namonode:9000/user/output/'
#output_dir1 = 'hdfs://x18201148-namonode:9000/user/output1/'
#hadoop_bin = '/opt/hadoop/bin/hadoop'

runner_type = 'inline'
output_dir = 'output/'
output_dir1 = 'outputdata/'
hadoop_bin = '/opt/hadoop/bin/hadoop'



size = None
dng_pr = None

count = 0
diff_pg_val_chk = None
###################################################################################################################



#####################################################################################################################
mr_job_linkgraph = MRCalculateLinkGraph(args=["link1.txt",'-r', runner_type, '--output', output_dir1])
with mr_job_linkgraph.make_runner() as runner_link:
    runner_link.run()

mr_graphsize = CountGraphNodesJob(args=["output/*" ,'-r', runner_type])
with mr_graphsize.make_runner() as runner_size:
    runner_size.run()
    for key, value in mr_graphsize.parse_output(runner_size.cat_output()):
        if size == None:
            size = value
    print("Initialising Size",size)      

startTime = datetime.now()
mr_initgraph = DistributeInitialPageRankJob(args=["output/*" ,'-r', runner_type,'--graphsize',size,'--output', output_dir1])
with mr_initgraph.make_runner() as runner_init:
    runner_init.run()

mr_dangling_pr = DanglingNodeJob(args=["output/*" ,'-r', runner_type])
with mr_dangling_pr.make_runner() as runner_dang:
    runner_dang.run()
    for key, value in mr_dangling_pr.parse_output(runner_dang.cat_output()):
        if dng_pr == None:
            dng_pr = value
    print("Initialising dangling",dng_pr)


mr_pagerank = PageRankJob(args=["output/*" ,'--graph-size',size,'--dangling-node-pr', dng_pr,'--damping-factor','.85', '-r', runner_type,'--output', output_dir, "--no-output"])
with mr_pagerank.make_runner() as runner_pr:
    runner_pr.run()

#####################################################################################################################
while True:
	count += 1

	mr_dangling_pr_iter = DanglingNodeJob(args=[output_dir+"*",'-r', runner_type])
	with mr_dangling_pr_iter.make_runner() as runner_dgpr_iter:
	    runner_dgpr_iter.run()
	    danglink_pg_val = None
	    for key, value in mr_dangling_pr_iter.parse_output(runner_dgpr_iter.cat_output()):
	        if danglink_pg_val == None:
	            danglink_pg_val = value
	    print("dangling",danglink_pg_val)

	

	mr_pagerank_iter = PageRankJob(args=[output_dir+"*" ,'--graph-size',size,'--dangling-node-pr', danglink_pg_val,'--damping-factor','.85', '-r', runner_type,'--output', output_dir, "--no-output"])
	with mr_pagerank_iter.make_runner() as runner_pr_iter:
	    runner_pr_iter.run()

	
	    
	mr_dif_iter = DiffPagerank(args=[output_dir+"*" ,'-r', runner_type])
	with mr_dif_iter.make_runner() as runner_dif_iter:
	    runner_dif_iter.run()
	    diff_pg_val = None
	    for key, value in mr_dif_iter.parse_output(runner_dif_iter.cat_output()):
	        if diff_pg_val == None:
	            diff_pg_val = value

	


	#if diff_pg_val_chk == None:
	#	diff_pg_val_chk = float(diff_pg_val)-0.1
	diff_pg_val_chk = 0.0005

	print("diff",float(diff_pg_val))
	print("iteration no.",count)
	if float(diff_pg_val) < diff_pg_val_chk or float(diff_pg_val) == 0.0:
		print("--------Result----------")
		print("Graph size",size)
		print("time :  ",datetime.now() - startTime)
		print("alpha",diff_pg_val_chk)
		print("iteration no.",count)
		break

