# 第13篇:Mybatis中打印Sql信息


>tip Sql打印需求
在Mybatis中如果我们要对我们的sql信息进行检查, 只能启动Spring容器, 去执行根据成功和失败来判断我们的逻辑是否有问题。
此时会比较耗时,因为要启动容器。基于这个痛点, 本文要设计一个工具。是我们不依赖Spring容器,也不依赖任何外部插件，直接就把
Sql信息的打印出来。


## 一、设计思路


![](https://img.springlearn.cn/blog/learn_1651310469000.png)

基于前面我们对Mybatis的学习,我们知道所有的sql信息,都会被解析成MappedStatement,并保存在 Configuration。
那么我们要做的

第一步就是解析sql信息成MappedStatement。而在Mybatis中的sql是可以写在Mapper.xml也可以使用注解形式,
直接写到接口类中的。

第二个知识点,Mybatis中是可以使用很多标签的如 <where/> <if/> <foreach/> <include/> 这些标签要先处理成sql信息。

第三步组装sql信息, 前面的学习我们知道sql信息如果是$变量符,那么会在直接会编译成sql信息。而动态sql是由DynamicSqlSource来直接解析参数
生成sql的。那么我们就需要将#占位符都调换成变量符,然后利用DynamicSqlSource给直接生成sql信息的。

第四步sql信息格式化。

第五步使用方法设计。

## 二、思路实现

## 2.1 MappedStatement解析

### 2.1.1 xml参数解析

```java 
private void loadMappedStatementByMapperFile(String mapperXmlFile) throws Exception {
    InputStream resourceAsStream = Resources.getResourceAsStream(mapperXmlFile);
    Map<String, XNode> sqlFragments = configuration.getSqlFragments();
    new XMLMapperBuilder(resourceAsStream, configuration, mapperXmlFile, sqlFragments).parse();
}
```

### 2.1.2 注解sql解析

```java 
private void loadMappedStatementByAnnotation() {
    MapperAnnotationBuilder mapperAnnotationBuilder =
        new MapperAnnotationBuilder(configuration, quickMapperChecker.mapper);
    mapperAnnotationBuilder.parse();
}
```

当执行完上面的代码,所有MappedStatement就生成了并保存到你指定的Configuration中了。


## 2.2 Sql中标签解析

### 2.2.1 Include 标签解析

拿到所有的sql执行标签"select|insert|update|delete",去执行include参数替换。
includeParser.applyIncludes(child.getNode());执行后 include 标签就替换成真正的sql片段了。
```java 
 private XNode findNode() throws Exception {
        InputStream resourceAsStream = Resources.getResourceAsStream(this.mapperFile);
        XPathParser xPathParser = new XPathParser(resourceAsStream);
        XNode mapperNode = xPathParser.evalNode("/mapper");
        List<XNode> children = mapperNode.getChildren();
        for (XNode child : children) {
            if (child.getStringAttribute("id").equals(quickMapperChecker.methodName)) {
                MapperBuilderAssistant mapperBuilderAssistant =
                    new MapperBuilderAssistant(configuration, quickMapperChecker.mapperFile);
                mapperBuilderAssistant.setCurrentNamespace(mapper.getName());
                XMLIncludeTransformer includeParser = new XMLIncludeTransformer(configuration, mapperBuilderAssistant);
                includeParser.applyIncludes(child.getNode());
                return child;
            }
        }
        // "select|insert|update|delete"
        return null;
    };
```

### 2.2.1 其他标签解析

```java 
 private void initNodeHandlerMap() {
    nodeHandlerMap.put("trim", new TrimHandler());
    nodeHandlerMap.put("where", new WhereHandler());
    nodeHandlerMap.put("set", new SetHandler());
    nodeHandlerMap.put("foreach", new ForEachHandler());
    nodeHandlerMap.put("if", new IfHandler());
    nodeHandlerMap.put("choose", new ChooseHandler());
    nodeHandlerMap.put("when", new IfHandler());
    nodeHandlerMap.put("otherwise", new OtherwiseHandler());
    nodeHandlerMap.put("bind", new BindHandler());
  }
```

这里我们要使用XMLScriptBuilder#parseDynamicTags。很可惜这个方法是受到保护的。
我们只能使用反射来对参数进行解析。


```java 
 // 解析xml中的标签信息
 Method parseDynamicTags = XMLScriptBuilder.class.getDeclaredMethod("parseDynamicTags", XNode.class);
 parseDynamicTags.setAccessible(true);

 XMLScriptBuilder xmlScriptBuilder = new XMLScriptBuilder(configuration, node);
 MixedSqlNode rootSqlNode = (MixedSqlNode)parseDynamicTags.invoke(xmlScriptBuilder, node);
```


### 2.2.2 bind参数生成

这里要说明下,我们举一个列子。以下面例子,我们拿到的参数是query。

```java 
List<OrderDO> list(@Param("query") OrderBatchEntityQuery query);
```

而他的xml比较复杂的。

```xml 
<select id="list" resultType="com.center.dal.entity.OrderDO">
        select *
        from order as ro
                 left join order_detail rod on ro.id = rod.replenish_order_id
        <where>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.ids)">
                and ro.id in
                <foreach collection="query.ids" open="(" separator="," index="index" item="id"
                         close=")">
                    #{id}
                </foreach>
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.orderCode)">
                and ro.order_code = #{query.orderCode}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.statusList)">
                and ro.status in
                <foreach collection="query.statusList" open="(" separator="," index="index" item="status"
                         close=")">
                    #{status}
                </foreach>
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.title)">
                and ro.title = #{query.title}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.salesWarehouseId)">
                and ro.sales_warehouse_id = #{query.salesWarehouseId}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.brandCode)">
                and ro.brand_code = #{query.brandCode}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.businessLineId)">
                and ro.business_line_id = #{query.businessLineId}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.signOwnerCode)">
                and ro.sign_owner_code = #{query.signOwnerCode}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.storageOwnerCode)">
                and ro.storage_owner_code = #{query.storageOwnerCode}
            </if>
            <if test="@com.center.dal.util.MybatisIfUtils@isNotEmpty(query.goodsBarcodes)">
                and rod.goods_barcode in
                <foreach collection="query.goodsBarcodes" open="(" separator="," index="index" item="goods_barcode"
                         close=")">
                    #{goods_barcode}
                </foreach>
            </if>
        </where>
    </select>

```

以上参数分为2部分,一部分是原始方法参数的解析。

![](https://img.springlearn.cn/blog/learn_1651311823000.png)

而BoundsSql中ParameterMapping是这样的。

![](https://img.springlearn.cn/blog/learn_1651311916000.png)

需要拿到参数中每个的数据信息。

```java 
 // 解析xml中的标签信息
 Method parseDynamicTags = XMLScriptBuilder.class.getDeclaredMethod("parseDynamicTags", XNode.class);
 parseDynamicTags.setAccessible(true);

 XMLScriptBuilder xmlScriptBuilder = new XMLScriptBuilder(configuration, node);
 MixedSqlNode rootSqlNode = (MixedSqlNode)parseDynamicTags.invoke(xmlScriptBuilder, node);
 DynamicContext context = new DynamicContext(configuration, namedParams);
 rootSqlNode.apply(context);
 // 标签信息参数解析
 Map<String, Object> bindings = context.getBindings();
```
![](https://img.springlearn.cn/blog/learn_1651312104000.png)

到这里复杂标签中的参数就获取到了。

## 2.3 占位符替换成变量符

### 2.3.1 占位符替换变量符

因为#占位符都会先调换成?。而参数都会按照顺序放在ParameterMapping中。

![](https://img.springlearn.cn/blog/learn_1651312283000.png)

这里我们要写代码将?替换成${ParameterMapping#getProperty}。

```java 
    /**
     * 处理占位符已经被替换成?的时候，用于将占位符重新替换成变量符
     *
     * @param sql
     *            占位符sql
     * @param index
     *            占位符当前处理的索引
     * @param parameterMappings
     *            占位符参数信息
     * @return String 变量符sql
     */
    private String resetSql(String sql, int index, List<ParameterMapping> parameterMappings, MetaObject metaObject) {
        int i = sql.indexOf("?");
        if (i > -1) {
            ParameterMapping parameterMapping = parameterMappings.get(index);
            String property = parameterMapping.getProperty();
            Class<?> javaType = parameterMapping.getJavaType();
            Object value = metaObject.getValue(parameterMapping.getProperty());
            String s;
            if (javaType.equals(String.class) || value instanceof String) {
                s = sql.replaceFirst("\\?", "\"\\${" + property + "}\"");
            } else {
                s = sql.replaceFirst("\\?", "\\${" + property + "}");
            }
            sql = resetSql(s, ++index, parameterMappings, metaObject);
        }
        return sql;
    }
```

### 2.3.2 生成Sql

利用变量符能直接生成sql的能力，我们直接将参数准备好，使用就好了。

```java 
 // 获取原始参数信息
 Object namedParams = paramNameResolver.getNamedParams(quickMapperChecker.args);
 // 复杂参数解析
 Map<String, Object> bindings = context.getBindings();
 // 标签参数 + 原始参数
 ((Map)namedParams).putAll(bindings);
 TextSqlNode textSqlNode = new TextSqlNode(resetSql(sql, 0, parameterMappings, metaObject));
 new DynamicSqlSource(configuration, textSqlNode).getBoundSql(namedParams).getSql());
```

## 2.4 sql格式化

这里我们就直接使用druid库中的sql格式化工具

```xml 
       <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid</artifactId>
            <version>1.2.6</version>
        </dependency>
```

这里因为我们知道是mysql数据库所以执行使用mysql格式化
```java
SQLUtils.formatMySql(boundSql.getSql());
```


## 2.5 使用方法设计

基于上门的代码,首先我们已经不依赖Spring容器了,所以要想分析sql就不用启动整个项目了。
直接将要分析的类和方法进行执行就行了。

```java 
OrderBatchEntityQuery query = JMockData.mock(OrderBatchEntityQuery.class);
// 如果需要绑定xml就使用bindMapper
QuickMapperChecker.analyse(QuickMapperChecker.mock(IOrderMapper.class).list(query))
   .bindMapper("mapper/center/ReplenishOrderMapper.xml").printSql();   
// 如果完全依赖注解跟简单
QuickMapperChecker.analyse(QuickMapperChecker.mock(IOrderMapper.class).list(query))
   .printSql();   
```


