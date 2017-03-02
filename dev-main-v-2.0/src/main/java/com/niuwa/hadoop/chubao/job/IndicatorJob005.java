package com.niuwa.hadoop.chubao.job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;
/**
 * 
 * 定价任务：<br> 
 * 获取用户rank值、所属城市等级及激活时间，
 * 用于确定用户可借款的初始额度
 * 输入来源：user_info
 * 输出字段：amt、user_id、daily_fee_rate、
 * @author maliqiang
 * @see 
 * @since 2016-6-21 
 */
public class IndicatorJob005 extends BaseJob {
    static Logger log = Logger.getLogger(IndicatorJob005.class);
	static Map<String, String> levelMap = new HashMap<String, String>();
	
	public static class PriceRuleMapper extends NiuwaMapper<Object, Text, NullWritable, Text>{
	    
	    public void  setup(Context context){
	        /**
	         * 读取cachefiles
	         * 
	         * 官方文档使用这个方法，测试使用上下文也能获取到，还不知道问题所在
	         * URI[] patternsURIs = Job.getInstance(context.getConfiguration()).getCacheFiles();
	         */
	        
	    	super.setup(context);
	    	BufferedReader reader= null;
	        try{

	        	URI[] paths =context.getCacheFiles();
		        log.info("[ cached file number ]{}"+ paths.length);
		        //Path cacheFilePath= new Path(paths[1].getPath());
		        Path cacheFilePath= new Path(paths[0].getPath());
		        reader= new BufferedReader(new FileReader(cacheFilePath.getName().toString()));
		        String str= null;
	            while((str= reader.readLine())!=null){
	                String[] spilts= str.split(";");
	                levelMap.put(spilts[0], spilts[2]);
	            }
	        }catch(Exception e){
	            e.printStackTrace();
	        }finally{
	            try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	    }
	    
	    
		/**
		 * @throws InterruptedException 
		 * @throws IOException 
		 * 
		 */
		protected void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			/**
			 * 该json对象包含以下字段：user_id（用户id）、user_rank（rank值）、device_this_phone（手机号）、
			 * device_activation（激活时间）、device_os_version（系统版本）、device_manufacturer（设备生产商）
			 * device_os（手机系统）、user_is_loan（是否贷款）、device_info（设备信息）、device_imei（IMEI编码）
			 * user_geo（地理位置）
			 * 2017/02/23添加字段：user_loan_overdue(触宝渠道判定三个月内借款逾期次数)
			 */
		    JSONObject userInfo = JSONObject.parseObject(value.toString());
		    JSONObject resultObj=new JSONObject();
			
			if(ChubaoJobConfig.isDebugMode() 
					||(Rules.isMatchedRule_1(userInfo.getLong("device_activation")))){
				
				String addrLevel = getAddrLevel(userInfo.getString("device_this_phone"));
				resultObj.put("user_id", userInfo.getString("user_id"));
				resultObj.put("device_activation", userInfo.getLong("device_activation"));
				resultObj.put("device_this_phone", userInfo.getString("device_this_phone"));
				resultObj.put("user_loan_overdue", userInfo.getIntValue("user_loan_overdue"));

				if("1".equals(addrLevel)){
					resultObj.put("user_base_amount", 1000.00);
				}else if("2".equals(addrLevel)){
					resultObj.put("user_base_amount", 800.00);
				}else{
					resultObj.put("user_base_amount",500.00);
				}
				//输出结果：可借初始金额、日手续费率、user_id
				context.write(NullWritable.get(),new Text(resultObj.toJSONString()));
			}
		}
	}
	
	
	/**
	 * 截取手机号除+86后的前7位,判断属于1、2、3线城市
	 * @param phoneNo 用户手机号
	 * @return 手机号所属城市的等级
	 */
	public static String getAddrLevel(String phoneNo){
		String phone = "";
		if(ChubaoUtil.telVilidate(phoneNo)){
			//phone = phoneNo.substring(3, 10);
			phone = getPhone(phoneNo);
		}
		String level=levelMap.get(phone);
		//如果存在匹配不到的，默认设置成3线城市
		return level==null?"3":level;
		
	}
	
	private static String getPhone(String phoneNo){
		String phone = "";
		
		if(phoneNo.startsWith("+861")){
			phone = phoneNo.substring(3, 10);
		}else if(phoneNo.startsWith("861")){
			phone = phoneNo.substring(2, 9);
		}else{
			phone = phoneNo.substring(0, 7);
		}
		
		return phone;
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
        
        job.setMapperClass(IndicatorJob005.PriceRuleMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
		
        // 缓存配置文件
        job.addCacheFile(ChubaoJobConfig.getConfigPath("static-mobile-tier.txt").toUri());
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_USER_INFO));
		// 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob005.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile( tempPaths.get(IndicatorJob005.class.getName()));
        
	}
	
}
