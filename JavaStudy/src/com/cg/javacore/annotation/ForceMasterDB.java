package com.cg.javacore.annotation;

import java.lang.annotation.*;

/**
 * 强制指定当前方法读取主库的数据，
 * 防止从从库读取发生的NPE问题（主从备份数据存在时间上的延迟),
 * 至于为什么加事务后，就会从主库读取，原因是公司mycat路由的时候做了特殊处理。
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
//@Transactional(propagation = Propagation.REQUIRED)
public @interface ForceMasterDB {
    /**
     * 配置数据对应的事务管理器bean的id
     * @return
     */
//    @AliasFor(annotation = Transactional.class, attribute = "value")
    String transactionManagerId();

}
