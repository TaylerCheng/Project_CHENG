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
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;
/**
 * 
 * 大额定价任务：<br> 
 * 获取用户所属城市等级，
 * 用于确定用户可借款的初始额度
 * 输入来源:user_info、STATIC/static-mobile-tier.txt
 * 输出字段:amt、user_id
 * @author maliqiang
 * @see 
 * @since 2016-6-24 
 */
public class LargeIndicatorJob003 extends BaseJob{
    static Logger log = Logger.getLogger(LargeIndicatorJob003.class);
	static Map<String, String> levelMap = new HashMap<String, String>();
	
	public static class LargePriceRuleMapper extends NiuwaMapper<Object, Text, NullWritable, Text>{
	    
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
			 */
		    JSONObject userInfo = JSONObject.parseObject(value.toString());
		    JSONObject resultObj=new JSONObject();
		
				String addrLevel = getAddrLevel(userInfo.getString("device_this_phone"));
				resultObj.put("user_id", userInfo.getString("user_id"));
				if("1".equals(addrLevel)){
					resultObj.put("amt", 2500.00);
				}else if("2".equals(addrLevel)){
					resultObj.put("amt", 2300.00);
				}else{
					resultObj.put("amt",2000.00);
				}
				//输出结果：可借初始金额、user_id
				context.write(NullWritable.get(),new Text(resultObj.toJSONString()));
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
			phone = phoneNo.substring(3, 10);
		}
		String level=levelMap.get(phone);
		//如果存在匹配不到的，默认设置成3线城市
		return level==null?"3":level;
		
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
        
        
        job.setMapperClass(LargeIndicatorJob003.LargePriceRuleMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        // 读取静态缓存的费率配置文件
        job.addCacheFile(ChubaoJobConfig.getConfigPath("static-mobile-tier.txt").toUri());
        
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_USER_INFO));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(LargeIndicatorJob003.class.getName()));
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(LargeIndicatorJob003.class.getName()));
        
	}
	
}
