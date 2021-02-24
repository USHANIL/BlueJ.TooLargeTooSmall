####################group0 wrapper ##########################3
#!/bin/bash

. /apps/rft/cmds/cecl_v2/environment.cfg

lp_date=$1
duration=$2
job_instance_id=$3

#lp_date=202001
#duration=48
#job_instance_id=00000

py_file_name=publish_common_tables_ctx.py
prop_file_name=${env}_med_cluster_02.ini

export PYSPARK_PYTHON=/apps/anaconda/4.3.1/3/bin/python
export PATH=/apps/anaconda/4.3.1/3/bin/:$PATH
export HADOOP_CONF_DIR=/etc/hive/conf:/etc/hbase/conf:

HOME=/apps/rft/cmds/cecl_v2/cecl_modeldevelopment/
sdl_grp_no=0

logPath=/data/rft/rfis/logs/cecl/
reportFile=sdl_group${sdl_grp_no}_execution_report.txt

distribution_list=CECL_DEV_Team@restricted.chase.com,CT_CCBRFT_RFIS_SRE_Team@restricted.chase.com

mail_success() {

echo "SDL group $sdl_grp_no $lp_date LP completed successfully in $env environment." | tee -a ${logPath}${reportFile}

Body1=" SDL group $sdl_grp_no $lp_date LP completed successfully in $env environment."
Body2=" Check execution report attached"
Body3=" Thanks"
Body4=" CECL Data Team"

echo -e "$Body1\n$Body2\n$Body3\n$Body4\n"
echo -e "$Body1\n$Body2\n$Body3\n$Body4\n" |  mailx -a ${logPath}${reportFile} -s "SDL group $sdl_grp_no $lp_date LP Execution Report" $distribution_list
}

mail_failure() {

echo "SDL group $sdl_grp_no $lp_date LP execution failed in $env environment." | tee -a ${logPath}${reportFile}

Body1=" SDL group $sdl_grp_no $lp_date LP execution failed in $env environment."
Body2=" Check execution report attached"
Body3=" Thanks"
Body4=" CECL CFM Data Team"

echo -e "$Body1\n$Body2\n$Body3\n$Body4\n"
echo -e "$Body1\n$Body2\n$Body3\n$Body4\n" |  mailx -a ${logPath}${reportFile} -s "SDL group $sdl_grp_no $lp_date LP Execution Report" $distribution_list
}


#SDL_PKG_RUN_PATH=/apps/rft/cmds/cecl_v2/cecl_modeldevelopment/
# cd $SDL_PKG_RUN_PATH
#source ./../cecl_modeldevelopment/environment/activate.sh
cd $HOME
source environment/activate.sh

python -m spowrk --settings $prop_file_name execute src/main/python/$py_file_name $lp_date $duration $env $job_instance_id

declare -A module_size_map=( ["cecl_data.sdl.etl.inputdata.staging.StagingBadArns"]="1200000000" \
                             ["cecl_data.sdl.etl.inputdata.staging.StagingInactiveClosuresTable"]="120000000" \
                             ["cecl_data.sdl.etl.processing.create_account_id_exclusions.FullAccountIDsToRemove"]="140000000" \
                             ["cecl_data.sdl.etl.processing.create_month_id_mapping_table.MonthIdMappingTable"]="160000" \
                             ["cecl_data.sdl.etl.processing.create_universal_customer_id_table.CreateUniversalCustomerID"]="2200000000" \
                             ["cecl_data.sdl.etl.processing.create_wamu_flag_accounts.AccountIDAssociatedWithWamu"]="90000000" \
                           )

grp_folder_count_ref=6
exit_code=0

rm -f ${logPath}${reportFile}
echo "SDL group $sdl_grp_no Execution Report" | tee -a ${logPath}${reportFile}
echo "============================" | tee -a ${logPath}${reportFile}

grp_folder_count=$(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/$lp_date | grep "^d" | wc -l)

if [ $grp_folder_count -ne $grp_folder_count_ref ]
then
    echo "Error: SDL Group $sdl_grp_no generated $grp_folder_count folders, expecting ${grp_folder_count_ref}" | tee -a ${logPath}${reportFile}
    echo "" | tee -a ${logPath}${reportFile}
    exit_code=1
fi

hdfs dfs -du -s /tenants/rft/rfis/conformed/cecl_v2/sdl/output/${lp_date}/* | while read line ; do
folder_size=$(echo ${line} | awk '{print $1}')
module=$( echo $line | cut -d' ' -f3- | cut -d'/' -f10- )
threshold=${module_size_map["$module"]}
#echo "module name: $module"
#echo "module size: $folder_size"
#echo "size threshold: $threshold"


if [ $folder_size -lt $threshold ] && [ $folder_size -ne 0 ]
then
    echo "Warning: $(echo $line | cut -d' ' -f3-)" | tee -a ${logPath}${reportFile}
    echo "module size: $(echo $folder_size | awk '{ foo = $1 / 1024 / 1024 ; print foo " MB" }' )" | tee -a ${logPath}${reportFile}
    echo "module size threshold: $(echo $threshold | awk '{ foo = $1 / 1024 / 1024 ; print foo " MB" }' )" | tee -a ${logPath}${reportFile}
    echo "" | tee -a ${logPath}${reportFile}

fi

if [ $folder_size -eq 0 ]
then
    echo "Error: $line" | tee -a ${logPath}${reportFile}
    echo "Module size: $folder_size, $module deleted from HDFS\n" | tee -a ${logPath}${reportFile}
    hdfs dfs -rm -r -f -skipTrash $(echo $line | cut -d' ' -f3-)
    echo "Remaining folders: $(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/$lp_date | grep "^d" | wc -l)" | tee -a ${logPath}${reportFile}
    echo "" | tee -a ${logPath}${reportFile}
    exit_code=1
fi
done

if [ $exit_code -eq 0 ]
then
    mail_success
else
    mail_failure
fi

exit $exit_code




##################################### inputdict #############################

# coding: utf-8

# In[6]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession, SQLContext, Window, types
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext, Window, types
from pyspark.sql.functions import udf
import json
import csv
import sys
from datetime import datetime as dt
import pandas as pd
from functools import reduce
from operator import and_
from pathlib import Path
from time import time
from datetime import *
from datetime import date
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
import subprocess
import click
spark = SparkSession.builder.appName("input dictionary").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

import pyspark.sql.functions as F

from concurrent import futures
from pyspark.sql import DataFrame

spark.conf.set("spark.sql.shuffle.partitions", 2500) 
spark.conf.set("spark.driver.memory","15g")
spark.conf.set("spark.executor.memory","15g")
spark.conf.set("spark.driver.cores","4")
spark.conf.set("spark.executor.cores","10")

# nas path
nas_dev="/tmp/i739937/rqd/nfs/nas_uat/input_dictionary"
nas_sit="/tmp/i739937/rqd/nfs/nas_uat/input_dictionary"
nas_uat="/rqd/nfs/nas_uat/input_dictionary/"
nas_prod="/rqd/cfm/nas_prd/input_dictionary/"
# nas path

launch_point_date=sys.argv[1]
cecl_ver = 'cecl_v2'
#launch_point_date="201906"
#version="V2"

launch_env=sys.argv[2]
#launch_env='uat'

if launch_env == 'uat':
    nas_path = nas_uat
elif launch_env == 'prod':
    nas_path = nas_prod
elif launch_env == 'dev':
    nas_path = nas_dev
elif launch_env == 'sit':
    nas_path = nas_sit

input_dict = "/apps/rft/cmds/" + cecl_ver + "/reports/output/input_dicts_"+ launch_point_date + "LP_"+ datetime.datetime.now().strftime("%Y-%m-%d-%H.%M.%S")+ ".txt"
TO_DL='CCAR_DL@jpmchase.com'
FROM_DL = 'CCAR_DL@jpmchase.com'

acq_10pc_hash = "fdl_10pc"
fcb_10pc_hash = "fdl_10pc"
fcb_100pc_hash = "fdl_100pc"
fcb_1pc_hash = "fdl_1pc"
fcb_01pc_hash = "fdl_01pc"
#
print("Hash Code for FdlAcquisitions fdl_10pc is " +  acq_10pc_hash)
print("Hash Code for FdlForecastBase fdl_10pc is " +  fcb_10pc_hash)
print("Hash Code for FdlForecastBase fdl_100pc is " +  fcb_100pc_hash)
print("Hash Code for FdlForecastBase fdl_1pc is " +  fcb_1pc_hash)
print("Hash Code for FdlForecastBase fdl_01pc is " +  fcb_01pc_hash)
base_path="/tenants/rft/rfis/conformed/" + cecl_ver + "/sdl/output/"
acq_10pc = spark.read.parquet(base_path + launch_point_date + "/cecl_data.fdl.fdl_pipeline.FdlAcquisitions/" + acq_10pc_hash +".parquet")
spark.conf.set("spark.sql.execution.arrow.enabled", "false")

fcb_dict = {"100pc":fcb_100pc_hash, "10pc":fcb_10pc_hash, "1pc":fcb_1pc_hash, "01pc":fcb_01pc_hash }


# In[7]:


def create_product_dicts(fdl_size):
    local_dict = dict()
    path = base_path + launch_point_date + "/cecl_data.fdl.fdl_pipeline.FdlForecastBase/{0}.parquet".format(fcb_dict[fdl_size])
    print(path)
    fdl_df = spark.read.parquet(path)
    fdl_pd = fdl_df.select("p5_code", "p8_code", "p16_code", "super_rollup_code").distinct().toPandas()
    p5_p8_dict = {k: g["p8_code"].unique().tolist() for k,g in fdl_pd.groupby("p5_code")}
    print(p5_p8_dict)
    p5_p16_dict = {k: g["p16_code"].unique().tolist() for k,g in fdl_pd.groupby("p5_code")}
    p5_p36_dict = {k: g["super_rollup_code"].unique().tolist() for k,g in fdl_pd.groupby("p5_code")}
    p8_p16_dict = {k: g["p16_code"].unique().tolist() for k,g in fdl_pd.groupby("p8_code")}
    p8_p36_dict = {k: g["super_rollup_code"].unique().tolist() for k,g in fdl_pd.groupby("p8_code")}
    p16_p36_dict = {k: g["super_rollup_code"].unique().tolist() for k,g in fdl_pd.groupby("p16_code")}
    product_dict = dict()
    cols = ["p5_code", "p8_code", "p16_code", "super_rollup_code"]
    for col in cols:
        product_dict[col] = fdl_pd[col].unique().tolist()

    local_dict["p5_p8_dict"] = p5_p8_dict
    local_dict["p5_p16_dict"] = p5_p16_dict
    local_dict["p5_p36_dict"] = p5_p36_dict
    local_dict["p8_p16_dict"] = p8_p16_dict
    local_dict["p8_p36_dict"] = p8_p36_dict
    local_dict["p16_p36_dict"] = p16_p36_dict
    local_dict["product_dict"] = product_dict    
    master_dict["fdl_"+fdl_size] = local_dict

    return master_dict

master_dict = dict()
for size in ["100pc","10pc","1pc","01pc"]:
    print("running for the size :: "+str(size))
    master_dict = create_product_dicts(size)

path = base_path + launch_point_date + "/cecl_data.fdl.fdl_pipeline.FdlForecastBase/{0}.parquet".format(fcb_dict["10pc"])
fdl_df = spark.read.parquet(path)

def create_monthend_dicts(fdl_df,master_dict):
    month_agg = dict(*fdl_df.select("monthend_date_timestamp", "month_id", "vintage")
                             .agg(F.max("month_id").alias("max_month_id"),
                                  F.min("month_id").alias("min_month_id"),
                                  F.max("vintage").alias("max_vintage"),
                                  F.min("vintage").alias("min_vintage"),
                                  F.max("monthend_date_timestamp").alias("max_monthend_date"),
                                  F.min("monthend_date_timestamp").alias("min_monthend_date"))
                             .toPandas()
                             .to_dict("records"))
    
    month_agg["max_monthend_date"] = dt.strftime(month_agg["max_monthend_date"], "%Y%m")
    month_agg["min_monthend_date"] = dt.strftime(month_agg["min_monthend_date"], "%Y%m")
    master_dict["month_agg"] = month_agg
    
    return master_dict


master_dict = create_monthend_dicts(fdl_df,master_dict)
master_dict["acq_cols"] = acq_10pc.columns
master_dict["fdl_cols"] = fdl_df.columns


with open(input_dict, "w") as file:
    file.write(json.dumps(master_dict))

""" 
def copyTohdfs(src_path,dest_path):
# copy the local file to hdfs path  
    p = subprocess.Popen([
        'hdfs',
        'dfs',
        '-copyFromLocal',
        str(src_path),str(dest_path)
    ])
    return_code = p.wait()
    print(return_code)
    if (return_code == 0):
       print("The file copied from " + src_path + " to " + dest_path + " Successfully")
    else :
       print("The file copy failed from " + src_path + "to" + dest_path )
def mkdirhdfs(path):
# copy the local file to hdfs path  
    p = subprocess.Popen([
        'hdfs',
        'dfs',
        '-mkdir',
        str(path)
    ])
    return_code = p.wait()
    print(return_code)
    if (return_code == 0):
       print("The directory " + path + " is created Successfully")
    else :
       print("The directory " + path + " creation is Failed" )	
"""
#copy to nas path
def copyToNas(src,dest):
    try:
        cmd = 'cp ' + src + ' ' + dest
        p = subprocess.call(cmd,shell=True)
        if (p == 0):
            print("The file copied from " + src + " to " + dest + " NAS Path Successfully")
        else :
            print("The file copy to NAS Path failed from " + src + "to" + dest )
    except Exception as e:
        print("Exception in copying to NAS Path ")+ str(e)
#copy to nas path

#hdfs_path= base_path + launch_point_date + "/input_dict"
#mkdirhdfs(hdfs_path)
#copyTohdfs(input_dict,hdfs_path)
copyToNas(input_dict,nas_path)


def send_notification(mail_subject,mail_from,mail_to,mail_host,mail_body,files):
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = mail_subject
            msg['From'] = mail_from
            msg['To'] = mail_to

            body = MIMEText(mail_body, 'html')
            for filename in files:
                fp = open(filename,'rb')
                att = MIMEApplication(fp.read())
                fp.close()
                att.add_header('Content-Disposition','attachment', filename=filename.split('/')[7])
                msg.attach(att)

            msg.attach(body)
            s = smtplib.SMTP(mail_host)
            s.starttls()
            s.sendmail(mail_from, mail_to, msg.as_string())
            s.close()
        except Exception as e:
            print("ERROR sending email ")+ str(e)

send_notification('CECL Input Dict for ' + launch_point_date,
                               FROM_DL,
                               TO_DL,
                               'mailhost.jpmchase.net',
                               'Attached the Input Dictionary for ' + launch_point_date + ' Launch Point Date ',
                               [input_dict])
 


########################################################################################################################
#Script : Run job in AWS and auto terminate it.                                                                        #
#Param :                                                                                                               #
#Author : Suresh Natarajan                                                                                             #
#Date   : 08-27-2020                                                                                                   #
########################################################################################################################

#!/bin/bash

. /apps/rft/cmds/cecl_v2/environment.cfg
echo " environment value :: $env"
logpath=/data/rft/rfis/logs/cecl

emr_coonfig_json_path=/apps/rft/cmds/cecl_v2/cecl_modeldevelopment/aws/emr/config
emr_wrapper_script_path=/apps/rft/cmds/cecl_v2/cecl_modeldevelopment/aws/emr
emr_wrapper_script=cecl_aws_emr_wrapper.sh
                 
DATE_WITH_TIME=`date "+%Y%m%d%H%M%S"`

                 
Step_Name=$1
step_param=$2

cd $emr_wrapper_script_path
$emr_wrapper_script_path/cecl_aws_emr_wrapper.sh launch cecl_aws_emr_launch.json $env

                    if [ $? -eq 0  ]
                    then
                    echo "The EMR launch is successful, proceeding with cleanup"
                    else
                    echo " The EMR launch is failed"
                    exit 1
                    fi


if [ "$Step_Name" = "aws_fdl_merge" ]
then
                    
	$emr_wrapper_script_path/cecl_aws_emr_wrapper.sh runjob cecl_aws_emr_fdl_merge.json $env $step_param $DATE_WITH_TIME
	#checking log file to see job completed successfully	
	logfile_name=runjob_cecl_aws_emr_fdl_merge_$DATE_WITH_TIME.log
	status=$( tail -n 3 $logpath/$logfile_name )
	echo $status | grep "Step Status after step completion: COMPLETED"

    if [[ $? == 0 ]]
    then
        echo "$Step_Name in EMR completed successfully"
      
     else
       echo "$Step_Name in EMR failed, aborting"
    fi

	 cd $emr_wrapper_script_path
	 $emr_wrapper_script_path/cecl_aws_emr_wrapper.sh terminate cecl_aws_emr_terminate.json $env
	   
    if [ $? -eq 0  ]
    then
    echo "The EMR Terminate is successful"
    exit 1
    else
    echo " The EMR Terminate is failed"
    exit 1
    fi

elif [ "$Step_Name" = "aws_fdl_dq" ]
	then
		echo " yet to work on "
		exit 0       
	       
else
	echo "incorrect step name passed"
	exit 1

fi


########################################################################################################################
#Script : Refresh the scenario and generate the metadata and input dictionary                                          #
#Param :                                                                                                               #
#Author : Suresh Natarajan                                                                                             #
#Date   : 04-15-2020                                                                                                   #
########################################################################################################################
. /apps/rft/cmds/cecl_v2/environment.cfg
export JAVA_HOME=/usr/java/latest
echo " environment value :: $env"
var_envrn=$env
job_id=$$
echo "job_id: $job_id"

HOME=/apps/rft/cmds/cecl_v2/cecl_modeldevelopment/
#HOME=/tmp/e859192/cecl_v2/cecl_modeldevelopment/
f3_json_path=/data/rft/rfis/landing/cecl/scenario_refresh/current_json/
f3_input_path=/data/rft/rfis/landing/cecl/scenario_refresh/input_json/
logpath=/data/rft/rfis/logs/cecl/
flagpath=/apps/rft/cmds/cecl_v2/reports
jar_path=/apps/rft/cmds/cecl_v2/
jar_file_name=ccar-cmds-framework-3.0.jar
lockfilename_ingestion="cecl_scenario_refresh_ingestion.lck"
logfilename_ingestion="cecl_scenario_refresh_ingestion_""$job_id"".log"
logfilename_scenario_refresh="cecl_scenario_refresh_sdl_fdl_""$job_id"".log"

failure_flag_ingestion=scenario_ingestion.failed
success_flag_ingestion=scenario_ingestion.success

#failure_flag_sdl_fdl=sdl_fdl_scenario_refresh.failed
failure_flag_sdl_fdl=publish_scenario.failed
#success_flag_sdl_fdl=sdl_fdl_scenario_refresh.success
success_flag_sdl_fdl=publish_scenario.succeed

f3_json_file_name=f3.json
pool_name=root.rfis_Pool
tgt_schema_name=db_rft_rfis_ccar
tgt_table_name=ccar_mev_scenarios_metadata
spark_application_name=F3RestDataLoad
master=yarn

## CBB Copy Parameters
CBB_REL_PATH=/apps/rft/cmds/cecl_v2/cbbcopy
STEP_NAME=rest_api_driver.py

#. /apps/rft/cmds/cecl/environment.cfg
#echo " environment value :: $env"

distribution_list=CCAR_DL@jpmchase.com,CFM_L3_Support@restricted.chase.com,Card_CECL_Model_Execution@restricted.chase.com,CT_CCBRFT_RFIS_SRE_Team@restricted.chase.com

mail_success() {

Body1=" Automated Scenario Ingestion completed successfully in $var_envrn environment."
Body2=" Input JSON = $(basename $oldest_json_file)"
Body3=" CBB output = $scr_dir_for_cfm"
Body4=" MTD output = $scr_dir_for_cfm"
Body5=" Thanks"
Body6=" CECL CFM Data Team"

echo -e "$Body1\n$Body2\n$Body3\n$Body4\n$Body5\n$Body6\n"

echo -e "$Body1\n$Body2\n$Body3\n$Body4\n\n$Body5\n$Body6" |  mailx -a $flagpath/F3RestDataLoadTracker.csv -s "Scenario Ingestion Automated Status Report" $distribution_list

}

mail_failure() {

Body1=" Automated Scenario Ingestion failed in $var_envrn environment."
Body2=" Input JSON = $(basename $oldest_json_file)"
Body3=" Failure Reason: $1"
Body4=" Log Location: $2"
Body5=" Action: $3"
Body6=" Thanks"
Body7=" CECL CFM Data Team"

echo -e "$Body1\n$Body2\n$Body3\n$Body4\n$Body5\n$Body6\n$Body7"

echo -e "$Body1\n$Body2\n$Body3\n$Body4\n$Body5\n$Body6\n$Body7" |  mailx -a $flagpath/F3RestDataLoadTracker.csv -s "Scenario Ingestion Automated Status Report" $distribution_list

}

refresh_kinit() {

if [ "$var_envrn" == "uat" ]
then
	export KRB5CCNAME=~/krb5cc_a_cmds_data_nu
    echo $(/opt/CARKaim/sdk/clipasswordsdk GetPassword -p AppDescs.AppID=90183-C-0 -p Query="safe=M1-AW-BA0-C-A-90183-000;Object=WinDomainQA-naeast.ad.jpmorganchase.com-a_cmds_data_nu" -o Password) | kinit a_cmds_data_nu@NAEAST.AD.JPMORGANCHASE.COM
else
    echo $(/apps/rft/cmds/gen_keytab_a_cmds_data_np.sh) | kinit a_cmds_data_np@NAEAST.AD.JPMORGANCHASE.COM
fi


}


house_keeping() {
echo "Data retention check on: $1"
today=$(date +'%s')
hdfs dfs -ls $1 | grep "^d" | while read line ; do
dir_date=$(echo ${line} | awk '{print $6}')
difference=$(( ( ${today} - $(date -d ${dir_date} +%s) ) / ( 24*60*60 ) ))
filePath=$(echo ${line} | awk '{print $8}')

refresh_kinit

if [ ${difference} -gt 30 ]; then
    hdfs dfs -rm -r -f -skipTrash $filePath
    echo "Removed: $filePath"
fi
done
}

process_scenario_ingestion() {
echo "In process_scenario_ingestion() "
if [ -f $flagpath/$failure_flag_ingestion ]
then
rm -f $flagpath/$failure_flag_ingestion
fi

if [ -f $flagpath/$success_flag_ingestion ]
then
rm -f $flagpath/$success_flag_ingestion
fi

if [ -f $flagpath/$failure_flag_sdl_fdl ]
then
rm -f $flagpath/$failure_flag_sdl_fdl
fi

if [ -f $flagpath/$success_flag_sdl_fdl ]
then
rm -f $flagpath/$success_flag_sdl_fdl
fi

if [ -f $flagpath/F3RestDataLoadTracker.csv ]
then
rm -f $flagpath/F3RestDataLoadTracker.csv
fi


if [ "$var_envrn" == "uat" ]
then
f3_epv_pwd=$kinit_f3_epv_uat
f3_epv_fid=AD\\V762867
else
f3_epv_pwd=$kinit_f3_epv_prd
f3_epv_fid=AD\\N727620
fi

target_dir="/tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012"

refresh_kinit

echo "hdfs dfs -rm -r -f -skipTrash $target_dir"
hdfs dfs -rm -r -f -skipTrash $target_dir

if [ $? -eq 0 ]
then
echo " hdfs directory $target_dir removed successfully"
else
echo " hdfs directory $target_dir delete is failed, aborting script "
mail_failure "Failed to remove HDFS tmp folder" "$target_dir" "Check HDFS permission, storage, restart job"
rm $flagpath/$lockfilename_ingestion
exit 1
fi

#check f3.json file available or not


echo "Check if $f3_json_path$f3_json_file_name present"
if [ -f $f3_json_path$f3_json_file_name ]
then
echo " f3.json file is present, good to proceed"
else
echo " f3.json file is not present, cant proceed, aborting the script "
mail_failure "f3.json file not found" ${f3_json_path} "Check folder: $f3_json_path, restart job"
rm $flagpath/$lockfilename_ingestion
exit 1
fi

refresh_kinit

echo "issuing spark-submit\n"
spark2-submit --master yarn --conf spark.ui.port=9091 --conf spark.driver.maxResultSize=8g --conf spark.kryoserializer.buffer.max=1G --conf spark.shuffle.service.enabled=true --queue root.RFIS_Pool --executor-cores=4 --executor-memory=16G --driver-memory=16G --num-executors=50 --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Dpool_name=RFIS_Pool -Denv=$var_envrn -Djavax.net.ssl.trustStore=/apps/rft/cmds/cecl_v2/context.jks -Djavax.net.ssl.trustStorePassword=changeit"  --files /apps/rft/cmds/cecl_v2/context.jks --class F3RestDataLoadNew $jar_path$jar_file_name $var_envrn $spark_application_name $master $pool_name $tgt_schema_name $tgt_table_name $f3_epv_fid $f3_epv_pwd $f3_json_path$f3_json_file_name $flagpath/ > $logpath$logfilename_ingestion


#spark2-submit --master yarn --conf spark.ui.port=4090 --conf spark.driver.maxResultSize=8g --conf spark.kryoserializer.buffer.max=1G --conf spark.shuffle.service.enabled=true --queue root.RFIS_Pool --executor-cores=4 --executor-memory=16G --driver-memory=16G --num-executors=50 --conf spark.dynamicAllocation.enabled=true --class F3RestDataLoadNew /tmp/e859192/f3_csvfile/ccar-cmds-framework-3.0.jar uat 'F3RestDataLoad' yarn root.rfis_Pool db_rft_rfis_ccar ccar_mev_scenarios_metadata AD\\V762867 3R7HpbELg2acVavau2xGn43X3fS26Q6N7g332vf75V6D36m6pkVXGJAcrSR8KuY /tmp/e859192/f3_csvfile/f3.json /tmp/e859192/ > /tmp/e859192/test.log


refresh_kinit

while :
do
    echo "Searching for ingestion flag files"
    if [ -f $flagpath/$failure_flag_ingestion ]
    then
    echo "Flag for failure file is present, ingestion failed, aboring script"
    echo "Check log: $logpath$logfilename_ingestion"
    mail_failure "$spark_application_name job failed" $logpath$logfilename_ingestion "Check log and F3RestDataLoadTracker.csv attached, restart job"
    rm $flagpath/$lockfilename_ingestion
    exit 1
    elif [ -f $flagpath/$success_flag_ingestion ]
    then
    echo "Success file found, ingestion completed fine , proeeding to sdl fdl build"
    #fn_email_ingestion_success
    break
    else
    sleep 30
    continue
    fi
done

echo "CECL: F3RestDataLoadNew completed $(date)"

############################# BLOCK 2 #############################################################################################################################################


cd $HOME
source environment/activate.sh
#python -m spowrk --settings publish_fdl.ini execute src/main/python/publish_scenario_refresh.py $lp_date 99 $env $job_id > $logpath$logfilename_scenario_refresh
#hdfs dfs -rm -r -f -skipTrash /tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012
refresh_kinit
python -m spowrk --settings publish_scenario.ini execute src/main/python/publish_scenario_auto_refresh.py 205012 350 $env $job_id &> $logpath$logfilename_scenario_refresh


while :
do
    echo "Searching for publish_scenario flag files"
    if [ -f $flagpath/$failure_flag_sdl_fdl ]
    then
    echo " flag for failure file is present , metadata table ingestion failed, aboring script"
    echo " Send email alert on failure "
    mail_failure "publish_scenario_auto_refresh job failed" "$logpath$logfilename_scenario_refreshi" "Check log, restart job"
    rm $flagpath/$lockfilename_ingestion
    exit 1
    elif [ -f $flagpath/$success_flag_sdl_fdl ]
    then
    echo " Success file found , metadata table ingestion completed fine , proeeding to sdl fdl build"
    echo " Send email alert on success "
    #fn_email_sdl_fdl_success
    break
    else
    sleep 30
    continue
    fi
done


#Validate fdl files
refresh_kinit
fdl_macro_cnt=$(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/ | grep ".parquet" | wc -l)
fdl_scnro_cnt=$(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/cecl_data.fdl.fdl_pipeline.FdlScenarios/ | grep ".parquet" | wc -l)
fdl_macro_size=$(hdfs dfs -du -s /tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/ | awk '{ print $1}')
fdl_scnro_size=$(hdfs dfs -du -s /tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/cecl_data.fdl.fdl_pipeline.FdlScenarios/ | awk '{ print $1}')

echo "fdl_macro_size= $fdl_macro_size, fdl_scnro_size= $fdl_scnro_size"

if [ $fdl_macro_cnt -eq 1 ] && [ $fdl_scnro_cnt -eq 1 ]
then
echo " Both scenario and macrofiles are present "
    if [ $fdl_macro_size -gt 400000 ] && [ $fdl_scnro_size -gt 100000000 ]
    then
    echo " Both scenario and macrofiles size looking good "
    else
    echo "  scenario or macrofiles size are not looking good"
    mail_failure "Scenarios or MacroFeatures file size mismatch" "/tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/" "Check logs, restart job"
    rm $flagpath/$lockfilename_ingestion
    exit 1
    fi

else
echo " scenario or/and macrofiles are missing "
mail_failure "Scenarios or MacroFeatures missing" "/tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/" "Check logs, restart job"
rm $flagpath/$lockfilename_ingestion
exit 1
fi

echo "CECL: Block 2 completed $(date)"

############################# BLOCK 3 #############################################################################################################################################
refresh_kinit
hdfs dfs -rm -r -f -skipTrash /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}_old

hdfs dfs -mv /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm} /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}_old

hdfs dfs -mkdir /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}
hdfs dfs -mkdir /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/latest_scenario


hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/205012/cecl_data.fdl.fdl_pipeline.* /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/



## Rename and replication
refresh_kinit
hdfs dfs -mv /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/*.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_100pc.parquet
hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_100pc.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_10pc.parquet
hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_100pc.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_1pc.parquet
hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_100pc.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/fdl_01pc.parquet


hdfs dfs -mv /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/*.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_100pc.parquet
hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_100pc.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_10pc.parquet
hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_100pc.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_1pc.parquet
hadoop distcp -Dmapred.job.queue.name=root.RFIS_Pool -m 100 /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_100pc.parquet /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/fdl_01pc.parquet

fdl_macro_cnt_aftr_repl=$(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlMacroFeatures/ | grep ".parquet" | wc -l)
fdl_scnro_cnt_aftr_repl=$(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/cecl_data.fdl.fdl_pipeline.FdlScenarios/ | grep ".parquet" | wc -l)




if [ $fdl_macro_cnt_aftr_repl -eq 4 ] && [ $fdl_scnro_cnt_aftr_repl -eq 4 ]
then
echo " four scenario and macrofiles are present after replication "
    else
echo " scenario or/and macrofiles are missing after replication "
mail_failure "Scenarios or MacroFeatures are missing after replication" "/tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}" "Check logs, restart job"
rm $flagpath/$lockfilename_ingestion
exit 1
fi


echo "CECL: Block 3 completed $(date)"


############################# BLOCK 4 #############################################################################################################################################

## Trigger input dictionary generation - business_date

#pyfile_name=input_dict_metadata_static_hashcode.py
#pyfile_name=single_input_dict_metadata.py
#pyfile_name=scenario_metadata_csv.py
pyfile_name=scenario_metadata_csv.py
pyfile_folder=/apps/rft/cmds/cecl_v2/reports/
logfile_name=input_dict_file_generation_${job_id}.log

export PYSPARK_PYTHON=/apps/anaconda/4.3.1/3/bin/python
export PATH=/apps/anaconda/4.3.1/3/bin/:$PATH
export HADOOP_CONF_DIR=/etc/hive/conf:/etc/hbase/conf:
refresh_kinit
spark2-submit --master=yarn --queue=root.RFIS_Pool --conf spark.ui.port=9092 --driver-memory=16g --executor-memory=30g --conf spark.yarn.executor.memoryOverhead=5g --num-executors=50 --executor-cores=5 --conf spark.yarn.submit.waitAppCompletion=true --jars /opt/cloudera/parcels/CDH/lib/hive/lib/metrics-core-3.0.2.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/zookeeper.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/htrace-core.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/guava-14.0.1.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hive-hbase-handler.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-server.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-protocol.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-hadoop-compat.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-hadoop2-compat.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-common.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-client.jar,/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-annotations.jar --conf spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hive/lib/metrics-core-3.0.2.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/zookeeper.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/htrace-core.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/guava-14.0.1.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hive-hbase-handler.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-server.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-protocol.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-hadoop-compat.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-hadoop2-compat.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-common.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-client.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hbase-annotations.jar: $pyfile_folder$pyfile_name scenario_processed/$scr_dir_for_cfm &> $logpath$logfile_name 


input_dict_txt_cnt=$(hdfs dfs -ls /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/${scr_dir_for_cfm}/input_dict/ | grep ".csv" | wc -l)

if [ $input_dict_txt_cnt -eq 1 ]
then
    echo " Found both input dictionary and metadata file "
else
    echo " input dictionary or metadata file is missing"
    mail_failure "Input dictionary or metadata file is missing " "$logpath$logfile_name " "Check logs, restart job"
    rm $flagpath/$lockfilename_ingestion
    exit 1
fi

hdfs dfs -rm -r -f -skipTrash /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/latest_scenario/latestscenario.json
echo "{latestscenario_folder:${scr_dir_for_cfm}}" | hdfs dfs -appendToFile - /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/latest_scenario/latestscenario.json

echo "CECL: Block 4 completed $(date)"


############################# BLOCK 5 #############################################################################################################################################

export PATH=/apps/anaconda/4.3.1/3/bin/:$PATH

cd /apps/rft/cmds/cecl_v2/cbbcopy
refresh_kinit

if [ "$var_envrn" == "uat" ]
then
# UAT path used to be through CST13, as CST13 is being dicommissioned, UAT data is now copied to ASH10 as well. Keep the IF branch for now just in case this may need to fallback.
# In the long term, UAT and PRD paths can be collapsed
    sh -x cecl_copy_to_mtd_or_cbb_from_sfp.sh cbb_ash_10 $job_id $scr_dir_for_cfm &> ${logpath}cecl_copy_sfp_to_cst4prd13_${job_id}.log
    test_cnt=$(grep "finished with a status code of FINISHED" ${logpath}cecl_copy_sfp_to_cst4prd13_${job_id}.log | wc -l)
    test $test_cnt -eq 2 && { echo "sfp to cst4_prd13 copy completed successfully"; } || { echo "sfp to cst4_prd13 copy failed, check log: ${logpath}cecl_copy_sfp_to_cst4prd13_${job_id}.log"; mail_failure "sfp to cst4_prd13 copy failed" "${logpath}cecl_copy_sfp_to_cst4prd13_${job_id}.log" "Check logs, restart job"; rm $flagpath/$lockfilename_ingestion; exit 1; }
    echo "CECL: SFP to CST13 copy completed $(date)"
else
# CBB ASH10 Copy
    sh -x cecl_copy_to_mtd_or_cbb_from_sfp.sh cbb_ash_10 $job_id $scr_dir_for_cfm &> ${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log
    test_cnt=$(grep "finished with a status code of FINISHED" ${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log | wc -l)
    test $test_cnt -eq 2 && { echo "sfp copy to as3_prd10 completed successfully"; } || { echo "sfp to as3_prd10 copy failed, check log: ${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log"; mail_failure "sfp copy to as3_prd10 failed" "${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log" "Check logs, restart job"; rm $flagpath/$lockfilename_ingestion; exit 1; }
    echo "CECL: SFP to ASH10 copy completed $(date)"
fi

# CBB to MTD Copy
sh -x cecl_copy_to_mtd_or_cbb_from_sfp.sh mtd $job_id $scr_dir_for_cfm &> ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log
test_cnt=$(grep "finished with a status code of FINISHED" ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log | wc -l)
test $test_cnt -eq 2 && { echo "ash3_prd10 to mtd copy completed successfully"; } || { echo "ash3_prd10 to mtd copy failed, check log: ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log"; mail_failure "ash3_prd10 to mtd copy failed" "${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log" "Check logs, restart job"; rm $flagpath/$lockfilename_ingestion; exit 1; }
echo "CECL: CBB to MTD copy completed $(date)"

# SFP to NAS Copy
sh -x cecl_copy_to_mtd_or_cbb_from_sfp.sh NAS $job_id $scr_dir_for_cfm &> ${logpath}cecl_copy_sfp_to_nas_${job_id}.log
test $? -eq 0 && { echo "sfp to nas copy completed successfully"; } || { echo "sfp to nas copy failed, check log: ${logpath}cecl_copy_sfp_to_nas_${job_id}.log"; mail_failure "sfp to nas copy failed" "${logpath}cecl_copy_sfp_to_nas_${job_id}.log" "Check logs, restart job"; rm $flagpath/$lockfilename_ingestion; exit 1; }
echo "CECL: SFP to NAS copy completed $(date)"

}

#### Main ####
if [ -f $flagpath/$lockfilename_ingestion ]
then
   echo "An instance of the job is already running, exit current one"
   exit 1
else
   touch  $flagpath/$lockfilename_ingestion
fi

while :
do
input_json_cnt=$(ls -l /data/rft/rfis/landing/cecl/scenario_refresh/input_json/*.json | wc -l)
if [ $input_json_cnt -eq 0 ]
then
    echo "Waiting for new input JSON file"
    sleep 300
    house_keeping /tenants/rft/rfis/conformed/cecl_v2/scenario_processed/
else
    echo "New input JSON file found: file count = $input_json_cnt"
    oldest_json_file=$(ls -lt /data/rft/rfis/landing/cecl/scenario_refresh/input_json/*.json | tail -1 | cut -d'/' -f2-)
    dos2unix /$oldest_json_file
    echo "Found input JSON file: /$oldest_json_file"

#    lp_date=$(echo $(head -n 1 /$oldest_json_file) | cut -d':' -f2-)
#    case $lp_date in
#        [[:digit:]][[:digit:]][[:digit:]][[:digit:]][[:digit:]][[:digit:]])
#            echo $lp_date
#            ;;
#        *)
#            echo "Launch Point parameter is expecting: yyyymm, received: $lp_date"
#            mail_failure "Launch Point line missing in JSON or malformated" "$oldest_json_file" "Update JSON file"
#            rm -f /$oldest_json_file
#    esac

#    tail -n +2 /$oldest_json_file > $f3_json_path$f3_json_file_name
    cp /$oldest_json_file ${f3_json_path}${f3_json_file_name}
    chmod 777 $f3_json_path$f3_json_file_name
    echo "CECL: Ingestion start time $(date)"
#    echo "Launch point: $lp_date"
    output_extension=$(date +"%Y%m%d_%H%M%S%Z")
#    scr_dir_for_cfm=scenario_${lp_date}LP_${output_extension}
    scr_dir_for_cfm=scenario_${output_extension}
    echo "Ouput folder: $scr_dir_for_cfm"
    echo "Start processing scenario ingestion"
    process_scenario_ingestion
    touch  /data/rft/rfis/landing/cecl/scenario_refresh/data_transfer/scenario.success
    chmod 777 /data/rft/rfis/landing/cecl/scenario_refresh/data_transfer/scenario.success
    hdfs dfs -mv /tenants/rft/rfis/conformed/cecl_v2/sdl/output/scenario_processed/$scr_dir_for_cfm /tenants/rft/rfis/conformed/cecl_v2/scenario_processed/
    hdfs dfs -rm -r -f -skipTrash /tenants/rft/rfis/conformed/cecl_v2/scenario_processed/latest_scenario/latestscenario.json
    echo "{latestscenario_folder:${scr_dir_for_cfm}}" | hdfs dfs -appendToFile - /tenants/rft/rfis/conformed/cecl_v2/scenario_processed/latest_scenario/latestscenario.json
    mv /$oldest_json_file /$oldest_json_file.${output_extension}
    mv /$oldest_json_file.${output_extension} /data/rft/rfis/landing/cecl/scenario_refresh/processed_json/.
    mail_success
    sleep 60
fi
done





#!/bin/bash
#####################################################################################################################
# Script Name   : cecl_cbb_pull_util.sh                                                                             #
# Purpose       : Copy data from CBB $cbb_pull_cbb_path to SFP $cbb_pull_sfp_path                                   #
# Usage         : sh cecl_cbb_pull_util.sh [CST4_PRD13 | ASH3_PRD10 | BOTH]  [ CBB_PATH ] [SFP_PATH] [ PROCESS_ID ] #
#####################################################################################################################

. /apps/rft/cmds/cecl_v2/environment.cfg
echo " environment value :: $env"
logpath=/data/rft/rfis/logs/cecl/

CBB_REL_PATH=/apps/rft/cmds/cecl_v2/cbbcopy
STEP_NAME=rest_api_driver.py

export PATH=/apps/anaconda/4.3.1/3/bin/:$PATH

function pull_data_from_cbb {
cbb_node_name=$1

echo "python  ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_node_name} --sig_name ${everest_to_cbb_sig_name} --run_id ${job_id_value} --script_name ${cbb_to_sfp_script_name} ${src_cbb_folder},${dst_sfp_folder},${cbb_krb},${cbb_keytab},${cbb_user},${everest_cluster_name},${cbb_to_sfp_copy_mode},${cbb_no_of_threads},${cbb_pull_validation_flag}"

python  ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_node_name} --sig_name ${everest_to_cbb_sig_name} --run_id ${job_id_value} --script_name ${cbb_to_sfp_script_name} ${src_cbb_folder},${dst_sfp_folder},${cbb_krb},${cbb_keytab},${cbb_user},${everest_cluster_name},${cbb_to_sfp_copy_mode},${cbb_no_of_threads},${cbb_pull_validation_flag} &> ${logpath}cbb_pull_common_${job_id_value}.log

test_cnt=$(grep "finished with a status code of FINISHED" ${logpath}cbb_pull_common_${job_id_value}.log | wc -l)
test $test_cnt -ne 1 && { echo "sfp pull from $cbb_node_name failed"; exit 1; } 
}

server_name=$1
src_cbb_folder=$2
dst_sfp_folder=$3
job_id_value=$4

if [ "$server_name" == "ASH3_PRD10" ]
then
    pull_data_from_cbb ${cbb_prd10_cluster_name} 
elif [ "$server_name" == "CST4_PRD13" ]
then
    pull_data_from_cbb ${cbb_prd13_cluster_name}
elif [ "$server_name" == "BOTH" ]
then
    pull_data_from_cbb ${cbb_prd10_cluster_name}
    pull_data_from_cbb ${cbb_prd13_cluster_name}
else
    echo "invalid server name, aborting script"
    exit 1
fi

exit 0


#!/bin/bash
#####################################################################################################
# Script Name	: cecl_aws_cbb_mtd_copy_util	        											#
# Purpose		: Copy the segment output folder to processed folder								#
# param			: sfp_folder_path sfp_folder_name sub_folder_derive_ind Del_flgub_folder_name_list 	#
# Author		: Sandhiya Arumugam																	#
# Created		: 05/14/2020				             											#
#####################################################################################################
#cecl_aws_cbb_mtd_copy_util.sh /tenants/rft/rfis/conformed/cecl_v2/AWS_DOWNLOAD/cfm_outputs/landing_sfp/segment_outputs/ 20190131 Y N 12345 N 
configpath=/apps/rft/cmds/cecl_v2
#Get environment variables
. $configpath/environment.cfg

logpath=/data/rft/rfis/logs/cecl/
log=$logpath"cecl_aws_cbb_mtd_copy_util_$job_id.log"

if [ $1 == "CBB_DEL" ]; then #CBB Del only
	cbb_delete_ind="Y"
	MTD_Copy="N"
	cbb_del_folder=$3
	job_id=$4
	if [ "$2" == "ASH10" ]
	then
		cbb_srvr=$cbb_prd10_cluster_name
	elif [ "$2" == "CST4" ]
	then
		cbb_srvr=$cbb_prd13_cluster_name
	fi
	echo "CBB Delete Only " > $log
else
	MTD_Copy="Y"
	cbb_srvr=$cbb_prd10_cluster_name	
	usage(){
			  echo "$0 sfp_folder_path sfp_folder_name sub_folder_derive_ind cbb_del_flg job_id cst_copy_ind target_folder sub_folder_name_list "
	}

	if [[ $# -lt 8 ]]; then
			usage
			echo "-- ERROR: MISSING PARAMETERS"
			echo "-- ERROR: 1.PARAMETER SFP FOLDER PATH $1"
			echo "-- ERROR: 2.PARAMETER SFP SUB FOLDER NAME $2"
			echo "-- ERROR: 3.PARAMETER SFP SUB FOLDER LIST DERIVE IND $3"
			echo "-- ERROR: 4.PARAMETER CBB DEL IND $4"
			echo "-- ERROR: 5.PARAMETER JOB ID $5"
			echo "-- ERROR: 6.PARAMETER CST COPY IND $6"
			echo "-- ERROR: 7.PARAMETER CBB FOLDER $7"
			echo "-- ERROR: 8.PARAMETER MTD FOLDER $8"
			echo "-- ERROR: 9.PARAMETER SFP SUB FOLDER LIST $9"
			exit 1
	else
		sfp_folder_path=$1
		sfp_folder_nm=$2
		sub_folder_derive_ind=$3
		cbb_delete_ind=$4
		job_id=$5
		cst_copy_ind=$6
		cbb_target_folder=$7
		mtd_target_folder=$8
		sub_folder_name_list=$9
	fi
fi
	echo "sfp_folder_path : $sfp_folder_path"
	echo "sfp_folder_nm : $sfp_folder_nm:"
	echo "sub_folder_name_list : $sub_folder_name_list"
	echo "sub_folder_derive_ind : $sub_folder_derive_ind"
	echo "cbb_delete_ind : $cbb_delete_ind"
	echo "cst_copy_ind : $cst_copy_ind"
	echo "job_id : $job_id"
	echo "cbb_target_folder : $cbb_target_folder"
	echo "mtd_target_folder : $mtd_target_folder"

#Set Path
#logpath=/data/rft/rfis/logs/cecl/
#log=$logpath"cecl_aws_cbb_mtd_copy_util_$job_id.log"
CBB_REL_PATH=/apps/rft/cmds/cecl_v2/cbbcopy



export PATH=/apps/anaconda/4.3.1/3/bin/:$PATH

echo "CECL: copy folder started at $(date '+%m%d%Y')" > $log


var_envrn=$env

echo "CECL-$0: Environment : $var_envrn" >> $log

## Copy Script Parameters
STEP_NAME=rest_api_driver.py
if [ "$MTD_Copy" == "Y" ] 
then
	if [ "$sub_folder_derive_ind" == "N"  ] 
	then
		echo "No need to derive"
		if [ -z "$sub_folder_name_list" ] 
		then
			echo "CECL-$0 date '+%m%d%Y' :  Sub folder list is empty"
			echo "CECL-$0 date '+%m%d%Y' :  Sub folder list is empty" >> $log
			exit 1
		fi
	else
		echo "CECL-$0 date '+%m%d%Y' : derive sub folder" >> $log
		#echo $kinit_id $kinit_epv_pwd
		#echo "$(/opt/CARKaim/sdk/clipasswordsdk GetPassword -p AppDescs.AppID=89055-A-0 -p Query="SAFE=M1-AW-BA0-A-A-89055-001;OBJECT=WinDomainNC-naeast.ad.jpmorganchase.com-a_rfis_conformed_nd" -o Password)" | kinit a_rfis_conformed_nd 
				
		sub_folder_name_list=`hadoop fs -ls $sfp_folder_path/$sfp_folder_nm | sed '1d;s/  */ /g' | cut -d\  -f8 |  awk -F/ '{print $NF}' | awk '{print}' ORS='~' | sed 's/.$//'` 
		echo "sub fodler list : $sub_folder_name_list" >>$log
	fi

	#cbb copy
	run_id=$sfp_folder_nm
	if [ "$MTD_Copy" == "Y" ] 
	then
		
		echo "CECL: SFP to ASH10 copy started $(date)"
		echo "CECL: SFP to ASH10 copy started $(date)" >>  $log

		echo "python ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_prd10_cluster_name} --sig_name ${everest_to_cbb_sig_name} --run_id ${run_id} --script_name ${everest_to_cbb_script_name} ${everest_to_cbb_copy_mode},${sfp_folder_path},${cbb_target_folder},${cbb_krb},${cbb_keytab},${cbb_user},${everest_cluster_name},${cbb_no_of_threads},${sfp_folder_nm},${sub_folder_name_list},${cbb_timeout},${cbb_validation_flag}"

		python ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_prd10_cluster_name} --sig_name ${everest_to_cbb_sig_name} --run_id ${run_id} --script_name ${everest_to_cbb_script_name} ${everest_to_cbb_copy_mode},${sfp_folder_path},${cbb_target_folder},${cbb_krb},${cbb_keytab},${cbb_user},${everest_cluster_name},${cbb_no_of_threads},${sfp_folder_nm},${sub_folder_name_list},${cbb_timeout},${cbb_validation_flag} &> ${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log
		
		test_cnt=$(grep "finished with a status code of FINISHED" "${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log" | wc -l)
		test $test_cnt -eq 1 && { echo "sfp copy to as3_prd10 finished" >> $log; rm ${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log; } || { echo "sfp copy to as3_prd10 failed, check log: ${logpath}cecl_copy_sfp_to_as3prd10_${job_id}.log" >> $log; exit 1; }


		echo "CECL: SFP to ASH10 copy completed $(date)"
		echo "CECL: SFP to ASH10 copy completed $(date)" >>  $log
		cbb_del_folder=$cbb_target_folder$sfp_folder_nm
		echo "cbb_del_folder :: $cbb_del_folder" >> $log
	fi

	#mtd copy

	if [ "$MTD_Copy" == "Y" ] 
	then
		
		echo "CECL: CBB to MTD copy started $(date)" >>  $log

		echo "python ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_prd10_cluster_name} --sig_name ${mtd_to_cbb_sig_name} --run_id ${run_id} --script_name ${mtd_to_cbb_script_name} ${cbb_target_folder}${sfp_folder_nm},hdfs://mt-dev/${mtd_target_folder}${sfp_folder_nm},${cbb_krb},${mtd_keytab},${mtd_user},${mtd_cluster_name},${cbb_to_mtd_copy_mode},${mtd_no_of_threads},${mtd_validation_flag},false,dummy,mtd"

		python ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_prd10_cluster_name} --sig_name ${mtd_to_cbb_sig_name} --run_id ${run_id} --script_name ${mtd_to_cbb_script_name} ${cbb_target_folder}${sfp_folder_nm},hdfs://mt-dev/${mtd_target_folder}${sfp_folder_nm},${cbb_krb},${mtd_keytab},${mtd_user},${mtd_cluster_name},${cbb_to_mtd_copy_mode},${mtd_no_of_threads},${mtd_validation_flag},false,dummy,mtd &> ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log

		test_cnt=$(grep "finished with a status code of FINISHED" ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log | wc -l)
		test $test_cnt -eq 1 && { echo "sfp copy to MTD finished" >> $log; rm ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log; } || { echo "sfp copy to MTD failed, check log: ${logpath}cecl_copy_ash3prd10_to_mtd_${job_id}.log" >.$log; exit 1; }
			
		echo "CECL: CBB to MTD copy completed $(date)" >>  $log
		
		if [ "$cst_copy_ind" == "Y" ]
		then
			echo "COPY TO CST"
		fi 
	fi 
fi
#CBB Delete
if [ "$cbb_delete_ind" == "Y" ]
then
	echo "DELETE IN CBB"
	echo "python ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_prd10_cluster_name} --sig_name ${everest_to_cbb_sig_name} --run_id ${run_id} --script_name ${cbb_del_script_name} $cbb_del_folder &> ${logpath}cecl_copy_ash3prd10_del_${job_id}.log"
	python ${CBB_REL_PATH}/${STEP_NAME} --cluster_name ${cbb_srvr} --sig_name ${everest_to_cbb_sig_name} --run_id 101000 --script_name ${cbb_del_script_name} $cbb_del_folder &> ${logpath}cecl_copy_ash3prd10_del_${job_id}.log
	test_cnt=$(grep "CbbDelete finished with a status code of FINISHED" ${logpath}cecl_copy_ash3prd10_del_${job_id}.log | wc -l)
	test $test_cnt -eq 1 && { echo "CBB Delete finished" >> $log; rm ${logpath}cecl_copy_ash3prd10_del_${job_id}.log; } || { echo "CBB Del ailed, check log: ${logpath}cecl_copy_ash3prd10_del_${job_id}.log" >$log; exit 1; }
	echo "CECL: CBB Delete completed $(date)" >>  $log
fi
