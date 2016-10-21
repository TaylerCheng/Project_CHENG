package com.cg.dubbox.api.service;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/**
 * 在WEB端或H5页面，对用户信息采集过程中，提供数据服务
 *
 * @author Cheng Guang on 2016/10/14.
 */

@Path("data")
@Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_XML })
@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_XML })
public interface DataRestService {

	/**
	 * 获取全国省市信息数据，提供给H5前端“院校所在地”选项框使用
	 *
	 * @return 学业认证结果
	 */
	@GET
	@Path("/cities")
	String getCitiesData();

	/**
	 * 根据城市代码获得位于该城市的所有院校，提供给H5前端“所在院校”选项框使用
	 *
	 * @return 学业认证结果
	 */
	@GET
	@Path("/academies/{citycode : \\d+}")
	String getAcademiesData(@PathParam("citycode") Integer cityCode);
}
