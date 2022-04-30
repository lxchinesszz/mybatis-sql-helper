package mybatis.sql.helper.core;

import com.alibaba.druid.sql.SQLUtils;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import mybatis.sql.helper.core.format.ColorConsole;
import mybatis.sql.helper.core.format.DatePatternEnum;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.annotation.MapperAnnotationBuilder;
import org.apache.ibatis.builder.xml.XMLIncludeTransformer;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.scripting.xmltags.*;
import org.apache.ibatis.session.Configuration;
import org.springframework.boot.ansi.AnsiColor;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.List;

/**
 * 无需启动容器对sql信息进行检查
 *
 * @author liuxin 2022/4/27 17:48
 */
@NoArgsConstructor
public class QuickMapperChecker {

    /**
     * 方法签名id
     */
    @Getter
    public String mapperId;

    @Setter
    public String methodName;

    /**
     * 方法参数
     */
    @Getter
    private Object[] args;

    /**
     * 参数解析器
     */
    @Getter
    private ParamNameResolver paramNameResolver;

    /**
     * mapper类型
     */
    private Class<?> mapper;

    /**
     * mybatis配置
     */
    @Getter
    private Configuration configuration;

    @Getter
    @Setter
    private String mapperFile;

    private boolean simple;

    public QuickMapperChecker(String mapperId, Object[] args, ParamNameResolver paramNameResolver, Class<?> mapper,
                              Configuration configuration) {
        this.mapperId = mapperId;
        this.args = args;
        this.paramNameResolver = paramNameResolver;
        this.mapper = mapper;
        this.configuration = configuration;
    }

    public static QuickMapperChecker proxy() {
        if (Objects.isNull(quickMapperChecker)) {
            quickMapperChecker = new QuickMapperChecker();
            quickMapperChecker.simple = true;
        }
        return quickMapperChecker;
    }

    private static QuickMapperChecker quickMapperChecker;

    private static final Map<Class<?>, Object> PRIMITIVE_WRAPPER_TYPE_MAP = new IdentityHashMap<>(8);

    static {
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Boolean.class, false);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Byte.class, 0);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Character.class, "");
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Double.class, 0D);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Float.class, 0L);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Integer.class, 0);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Long.class, 0L);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Short.class, 0);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(Void.class, Void.TYPE);

        PRIMITIVE_WRAPPER_TYPE_MAP.put(boolean.class, false);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(byte.class, 0);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(char.class, "");
        PRIMITIVE_WRAPPER_TYPE_MAP.put(double.class, 0D);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(float.class, 0L);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(int.class, 0);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(long.class, 0L);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(short.class, 0);
        PRIMITIVE_WRAPPER_TYPE_MAP.put(void.class, null);
    }

    private static Class<?>[] interfacesFromMapper(Class<?> mapper) {
        Class<?>[] interfaces = mapper.getInterfaces();
        List<Class<?>> interfacesClass = new ArrayList<>();
        if (interfaces.length > 0) {
            interfacesClass.addAll(Arrays.asList(interfaces));
        }
        if (mapper.isInterface()) {
            interfacesClass.add(mapper);
        }
        return interfacesClass.toArray(new Class[]{});
    }

    public static <T> T mock(Class<T> mapper) throws Exception {
        return mock(mapper, new Configuration());
    }

    @SuppressWarnings("unchecked")
    public static <T> T mock(Class<T> mapper, Configuration configuration) throws Exception {
        return (T) Proxy.newProxyInstance(mapper.getClassLoader(), interfacesFromMapper(mapper),
                (proxy, method, args) -> {
                    String mapperId = method.getDeclaringClass().getName() + "." + method.getName();
                    if (Objects.isNull(quickMapperChecker)) {
                        quickMapperChecker = new QuickMapperChecker(mapperId, args,
                                new ParamNameResolver(configuration, method), mapper, configuration);
                        quickMapperChecker.setMethodName(method.getName());
                    } else {
                        boolean simple = quickMapperChecker.simple;
                        quickMapperChecker = new QuickMapperChecker(mapperId, args,
                                new ParamNameResolver(configuration, method), mapper, configuration);
                        quickMapperChecker.simple = simple;
                        quickMapperChecker.setMethodName(method.getName());
                    }
                    Class<?> returnType = method.getReturnType();
                    Object result = PRIMITIVE_WRAPPER_TYPE_MAP.get(returnType);
                    if (quickMapperChecker.simple) {
                        quickMapperChecker.printSql();
                    }
                    return Objects.nonNull(result) ? result : new DefaultObjectFactory().create(returnType);
                });
    }

    /**
     * 处理占位符已经被替换成?的时候，用于将占位符重新替换成变量符
     *
     * @param sql               占位符sql
     * @param index             占位符当前处理的索引
     * @param parameterMappings 占位符参数信息
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

    /**
     * sql打印
     *
     * @return String
     * @throws Exception 未知异常
     */
    public String getSql() throws Exception {
        if (!StringUtils.isBlank(this.mapperFile)) {
            loadMappedStatementByMapperFile(this.mapperFile);
        }
        loadMappedStatementByAnnotation();
        boolean hasMapped = configuration.hasStatement(quickMapperChecker.mapperId);
        if (!hasMapped) {
            throw new RuntimeException(
                    "未找到MappedStatement,请检查是否需要绑定mapper xml文件:[" + quickMapperChecker.mapperId + "]");
        }
        MappedStatement mappedStatement = configuration.getMappedStatement(quickMapperChecker.mapperId);
        SqlSource sqlSource = mappedStatement.getSqlSource();
        Object namedParams = paramNameResolver.getNamedParams(quickMapperChecker.args);
        BoundSql boundSql = mappedStatement.getBoundSql(namedParams);
        // 占位符
        if (sqlSource instanceof RawSqlSource || sqlSource instanceof DynamicSqlSource) {
            // 占位sql，将#替换成$
            String sql = boundSql.getSql();
            List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
            XNode node = findNode();
            if (Objects.nonNull(node)) {
                // 解析xml中的标签信息
                Method parseDynamicTags = XMLScriptBuilder.class.getDeclaredMethod("parseDynamicTags", XNode.class);
                parseDynamicTags.setAccessible(true);

                XMLScriptBuilder xmlScriptBuilder = new XMLScriptBuilder(configuration, node);
                MixedSqlNode rootSqlNode = (MixedSqlNode) parseDynamicTags.invoke(xmlScriptBuilder, node);
                DynamicContext context = new DynamicContext(configuration, namedParams);
                rootSqlNode.apply(context);
                // 标签信息参数解析
                Map<String, Object> bindings = context.getBindings();
                // 标签参数 + 原始参数
                ((Map) namedParams).putAll(bindings);
            }
            MetaObject metaObject = configuration.newMetaObject(namedParams);
            processDate(parameterMappings, metaObject);
            TextSqlNode textSqlNode = new TextSqlNode(resetSql(sql, 0, parameterMappings, metaObject));
            return SQLUtils
                    .formatMySql((new DynamicSqlSource(configuration, textSqlNode).getBoundSql(namedParams).getSql()));
        } else {
            return SQLUtils.formatMySql(boundSql.getSql());
        }
    }

    private void processDate(List<ParameterMapping> parameterMappings, MetaObject metaObject) {
        for (ParameterMapping parameterMapping : parameterMappings) {
            String property = parameterMapping.getProperty();
            Object value = metaObject.getValue(property);
            if (value instanceof Date) {
                metaObject.setValue(property, DatePatternEnum.DATE_TIME_PATTERN.format((Date) value));
            }
        }
    }

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
    }

    ;

    private void loadMappedStatementByAnnotation() {
        MapperAnnotationBuilder mapperAnnotationBuilder =
                new MapperAnnotationBuilder(configuration, quickMapperChecker.mapper);
        mapperAnnotationBuilder.parse();
    }

    private void loadMappedStatementByMapperFile(String mapperXmlFile) throws Exception {
        InputStream resourceAsStream = Resources.getResourceAsStream(mapperXmlFile);
        Map<String, XNode> sqlFragments = configuration.getSqlFragments();
        new XMLMapperBuilder(resourceAsStream, configuration, mapperXmlFile, sqlFragments).parse();
    }

    public void printSql() throws Exception {
        ColorConsole.colorPrintln("🚀 格式化SQL:");
        ColorConsole.colorPrintln(AnsiColor.BRIGHT_MAGENTA, "{}", getSql());
    }

    /**
     * sql信息进行检查
     *
     * @param t   泛型
     * @param <T> 泛型
     * @return QuickMapperChecker
     */
    public static <T> QuickMapperChecker analyse(T t) {
        // 1. 调用方法
        return quickMapperChecker;
    }

    /**
     * 绑定mapper文件
     *
     * @param mapperFile mapper文件地址
     * @return QuickMapperChecker
     */
    public QuickMapperChecker bindMapper(String mapperFile) {
        quickMapperChecker.setMapperFile(mapperFile);
        return quickMapperChecker;
    }
}
