package org.java.plus.dag.core.base.utils;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import com.taobao.recommendplatform.protocol.solution.Context;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.solution.Context;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * @author seven.wxy
 * @date 2018/10/10
 */
public class Logger {
    private static final String LOG_LEVEL_CONFIG_KEY = "logLevel";

    private static final ThreadLocal<List<Map.Entry<Level, String>>> LINE_LIST_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> IS_DEBUG = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Level> LEVEL = ThreadLocal.withInitial(() -> Level.ERROR);
    private static final ThreadLocal<Integer> LOG_STACK_LINE = ThreadLocal.withInitial(() -> 3);

    public static String getLogLevel(Context context) {
        String solutionLogLevel = SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_LOG_LEVEL);
        return ContextUtil.getStringOrDefault(context, LOG_LEVEL_CONFIG_KEY,
            StringUtils.defaultIfEmpty(solutionLogLevel, Level.ERROR.name()));
    }

    public static void init(ProcessorContext context) {
        IS_DEBUG.set(context.getDebug());
        LEVEL.set(context.isDisableLog() ? Level.OFF : Level.get(context.getLogLevel()));
        LOG_STACK_LINE.set(context.getLogStackLine());
        if (context.getDebug()) {
            LINE_LIST_LOCAL.set(Lists.newArrayList());
        }
    }

    public static <T> void info(Supplier<T> log) {
        // debug mode info log
        if (IS_DEBUG.get() && levelMatch(Level.INFO)) {
//            ServiceFactory.getSolutionLogger().info(log.get().toString());
            LINE_LIST_LOCAL.get().add(new AbstractMap.SimpleEntry(Level.INFO, ObjectUtils.defaultIfNull(log.get(), StringUtils.EMPTY)));
        }
    }

    public static <T> void warn(Supplier<T> log) {
        // debug mode warn log
        if (IS_DEBUG.get() && levelMatch(Level.WARN)) {
//            ServiceFactory.getSolutionLogger().warn(log.get().toString());
            LINE_LIST_LOCAL.get().add(new AbstractMap.SimpleEntry(Level.WARN, ObjectUtils.defaultIfNull(log.get(), StringUtils.EMPTY)));
        }
    }

    public static void error(String log, Throwable t) {
        error(() -> log, t);
    }

    public static <T> void error(Supplier<T> log, Throwable t) {
        if (Objects.nonNull(t) && Objects.nonNull(log.get()) && levelMatch(Level.ERROR)) {
//            ServiceFactory.getSolutionLogger().error(Objects.toString(log.get()), t);
            if (IS_DEBUG.get()) {
                LINE_LIST_LOCAL.get().add(new AbstractMap.SimpleEntry(Level.ERROR, log.get()));
                LINE_LIST_LOCAL.get().add(new AbstractMap.SimpleEntry(Level.ERROR, ExceptionUtils.getStackTrace(t)));
            }
        }
    }

    public static <T> void error(Supplier<T> log) {
        if (Objects.nonNull(log.get()) && levelMatch(Level.ERROR)) {
//            ServiceFactory.getSolutionLogger().warn(Objects.toString(log.get()));
            if (IS_DEBUG.get()) {
                LINE_LIST_LOCAL.get().add(new AbstractMap.SimpleEntry(Level.ERROR, ObjectUtils.defaultIfNull(log.get(), StringUtils.EMPTY)));
            }
        }
    }

    public static void onlineWarn(String log) {
        onlineWarn(() -> log);
    }

    public static <T> void onlineWarn(Supplier<T> log) {
        if (levelMatch(Level.WARN)){
//            ServiceFactory.getSolutionLogger().warn(getCodeLine() + log.get());
            if (IS_DEBUG.get()) {
                LINE_LIST_LOCAL.get().add(new AbstractMap.SimpleEntry(Level.WARN, ObjectUtils.defaultIfNull(log.get(), StringUtils.EMPTY)));
            }
        }
    }

    public static boolean levelMatch(Level level) {
        return Objects.nonNull(LEVEL.get()) && level.right <= LEVEL.get().right;
    }

    public static boolean isPrint() {
        return IS_DEBUG.get();
    }

    enum Level {
        OFF(0), ERROR(1), WARN(2), INFO(3), DEBUG(4);
        int right;

        Level(int right) {
            this.right = right;
        }

        public static Level get(String value) {
            for (Level level : Level.values()) {
                if (level.name().equalsIgnoreCase(value)) {
                    return level;
                }
            }
            return WARN;
        }
    }

    public static List<String> getLogDetail() {
        List<String> logList = null;
        List<Map.Entry<Level, String>> lineList = LINE_LIST_LOCAL.get();
        if (lineList != null) {
            logList = Lists.newArrayList();
            for (Map.Entry<Level, String> line : lineList) {
                logList.add(StringUtils.join(line.getKey(), StringUtils.SPACE, line.getValue()));
            }
        }
        return logList;
    }

    private static String getCodeLine() {
        int line = LOG_STACK_LINE.get();
        Throwable throwable = new Throwable();
        if (throwable.getStackTrace().length < line + 1) {
            return StringUtils.EMPTY;
        } else {
            StackTraceElement ste = throwable.getStackTrace()[line];
            return ste.getFileName() + ": Line " + ste.getLineNumber() + " ";
        }
    }

    public static void clear() {
        LINE_LIST_LOCAL.remove();
        IS_DEBUG.remove();
        LEVEL.remove();
        LOG_STACK_LINE.remove();
    }

    public static void initDebugLog() {
        IS_DEBUG.set(true);
        LINE_LIST_LOCAL.set(Lists.newArrayList());
        LEVEL.set(Level.INFO);
    }

}
