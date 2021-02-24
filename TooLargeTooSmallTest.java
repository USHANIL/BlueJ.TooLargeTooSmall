

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
