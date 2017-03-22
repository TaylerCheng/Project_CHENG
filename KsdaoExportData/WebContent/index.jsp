<%@ page language="java" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>数据导出系统</title>
</head>
<body>
<div style="width:100%;height:100%;text-align:center;vertical-align:middle;padding-top:20px;">
    <B>POS贷数据导出系统</B>
    <form action="servlet/ExcuteHiveCmdServlet" method="post">
        地区<input type="text" value = "南京" name="mcht_place"><br/>    
        入网时间  ><input type="text" value = "6" name="enter_web_month"><br/>
        近6个月月均交易额  ><input type="text" value = "40000" name="latest_6_avg_month_amount"><br/>
        近6个月月均交易笔数  ><input type="text" value = "5" name="latest_6_avg_month_num"><br/>
        使用条件<input type="radio" name="useCondition" value="yes" ><br/>    
    <input type="submit" value="提交">       
    </form>
</div> 

</body>
</html>