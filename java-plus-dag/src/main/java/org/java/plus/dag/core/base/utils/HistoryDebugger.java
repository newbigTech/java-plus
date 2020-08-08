package org.java.plus.dag.core.base.utils;

//import com.alibaba.alimonitor.jmonitor.utils.IPUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.taobao.recommendplatform.protocol.solution.Context;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.proc.IDebugDataSample;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.func.FuncUtils;
import org.java.plus.dag.solution.Context;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.java.plus.dag.core.base.constants.ConstantsFrame.NULL_JSON_ARRAY;
import static org.java.plus.dag.core.base.constants.ConstantsFrame.NULL_JSON_OBJECT;
import static org.java.plus.dag.core.base.constants.ConstantsFrame.TYPE_PROCESSOR;
import static org.java.plus.dag.core.base.model.ProcessorContext.UTDID_NAME;
import static org.java.plus.dag.core.base.utils.func.FuncUtils.FuncType;
import static org.java.plus.dag.core.base.utils.func.FuncUtils.FuncType.ctx;
import static org.java.plus.dag.core.base.utils.func.FuncUtils.FuncType.extData;

/**
 * @author youku
 */
@SuppressWarnings("all")
public class HistoryDebugger {

    private static final String IS_OPEN = "isOpen";
    private static final String CONFIG = "config";
    private static final String CLASS = "class";
    private static final String RATE = "rate";
    private static final String OR = "or";
    private static final String DURATION = "duration";
    private static final String IP = "ip";
    private static final String PERIOD = "period";
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static class SampleRate {
        private static final SampleRate FALSE_SAMPLE = new SampleRate();
        private volatile int hashcode;
        private double rate;
        private Set<String> ip;
        private Set<String> utdid;
        private long[] duration;
        private long[] perioid;
        private Function<ProcessorContext, Boolean> processor;
        List<Map<Object, Double>> rateList;
        List<Pair<FuncType, List<Function<Object, Boolean>>>> sampleList;
        private boolean or = true;

        public SampleRate setHashcode(int hashcode) {
            this.hashcode = hashcode;
            return this;
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private static final LoadingCache<String, SampleRate> SAMPLE_CACHE = CacheBuilder
        .newBuilder()
        .concurrencyLevel(Runtime.getRuntime().availableProcessors())
        .softValues()
        .build(new CacheLoader<String, SampleRate>() {
            @Override
            @ParametersAreNonnullByDefault
            public SampleRate load(String key) {
                JSONObject jsonObject = JsonUtils.getParam(TppConfigUtil.getJson(SolutionConfig.SOLUTION_CONFIG), SolutionConfig.DEBUG_SAMPLE);
                SampleRate sampleRate = SampleRate.FALSE_SAMPLE;
                if (JsonUtils.getParam(jsonObject, IS_OPEN, false)) {
                    initSampleRate(jsonObject, (sampleRate = new SampleRate()).setHashcode(jsonObject.hashCode()));
                }
                return sampleRate;
            }
        });

    private static void initSampleRate(JSONObject jsonObject, SampleRate sampleRate) {
        sampleRate.rate = JsonUtils.getParam(jsonObject, RATE, 0.0D);
        sampleRate.utdid = StrUtils.strToStrSet(jsonObject.getString(UTDID_NAME), StringPool.COMMA);
        sampleRate.or = JsonUtils.getParam(jsonObject, OR, true);
        sampleRate.ip = StrUtils.strToStrSet(jsonObject.getString(IP), StringPool.COMMA);
        String[] arr = StringUtils.split(StringUtils.trimToEmpty(jsonObject.getString(DURATION)), StringPool.C_TILDA);
        if (Array.getLength(arr) == 2) {
            try {
                long time = Timestamp.valueOf(LocalDateTime.parse(arr[0], DATETIME_FORMATTER)).getTime();
                int interval = NumberUtils.toInt(arr[1]);
                if (interval > 0) {
                    sampleRate.duration = new long[]{time, time + interval * 60 * 1000};
                }
            } catch (Exception exp) {
                //ignore
            }
        }
        arr = StringUtils.split(StringUtils.trimToEmpty(jsonObject.getString(PERIOD)), StringPool.C_TILDA);
        if (Array.getLength(arr) == 2) {
            try {
                sampleRate.perioid = new long[]{Timestamp.valueOf(LocalDateTime.parse(arr[0], DATETIME_FORMATTER)).getTime(),
                    Timestamp.valueOf(LocalDateTime.parse(arr[1], DATETIME_FORMATTER)).getTime()};
            } catch (Exception exp) {
                //ignore
            }
        }
        createFuncs(jsonObject, sampleRate);
    }

    private static Boolean checkIp(SampleRate sampleRate) {
        return CollectionUtils.isEmpty(sampleRate.ip) ? null : true;//sampleRate.ip.contains(IPUtils.getLocalIp());
    }

    private static Boolean checkUtdid(SampleRate sampleRate, ProcessorContext context) {
        return CollectionUtils.isEmpty(sampleRate.utdid) ? null : sampleRate.utdid.contains(context.getUtdid());
    }

    private static Boolean checkDuration(SampleRate sampleRate) {
        if (ArrayUtils.isEmpty(sampleRate.duration)) {
            return null;
        }
        long[] arr = sampleRate.duration;
        if (arr.length == 2) {
            long lo = System.currentTimeMillis();
            return lo >= arr[0] && lo <= arr[1];
        }
        return false;
    }

    private static Boolean checkProcessor(SampleRate sampleRate, ProcessorContext context) {
        return sampleRate.processor == null ? null : BooleanUtils.toBoolean(sampleRate.processor.apply(context));
    }

    private static Boolean checkPeriod(SampleRate sampleRate) {
        if (ArrayUtils.isEmpty(sampleRate.perioid)) {
            return null;
        }
        long[] arr = sampleRate.perioid;
        if (arr.length == 2) {
            long lo = System.currentTimeMillis();
            return lo >= arr[0] && lo <= arr[1];
        }
        return false;
    }

    private static Boolean checkRate(SampleRate sampleRate, ProcessorContext context, double randomDouble) {
        List<Pair<FuncType, List<Function<Object, Boolean>>>> sampleList = ListUtils.emptyIfNull(sampleRate.sampleList);
        List<Map<Object, Double>> rateList = ListUtils.emptyIfNull(sampleRate.rateList);
        Boolean result = null;
        if (!sampleList.isEmpty() && sampleList.size() == rateList.size()) {
            for (int i = 0, size = sampleList.size(); i < size; i++) {
                Pair<FuncType, List<Function<Object, Boolean>>> pair = sampleList.get(i);
                int j = i;
                Stream stream = ListUtils.emptyIfNull(pair.getValue())
                    .stream();
                if (pair.getKey() == ctx) {
                    stream = stream.map(f -> rateList.get(j).get(((Function<Object, Boolean>) f).apply(context)))
                        .filter(Objects::nonNull);
                } else if (pair.getKey() == extData) {
                    stream = stream.map(f -> rateList.get(j).get(((Function<Object, Boolean>) f).apply(context)))
                        .filter(Objects::nonNull);
                } else {
                    stream = null;
                }
                if (stream != null) {
                    if (sampleRate.or) {
                        result = (boolean) stream.filter(d -> MathUtil.lessThan(randomDouble, (double) d)).map(d -> Boolean.TRUE).findFirst().orElse(Boolean.FALSE);
                        if (result) {
                            return true;
                        }
                    } else {
                        result = stream.allMatch(d -> MathUtil.lessThan(randomDouble, (double) d));
                        if (!result) {
                            return false;
                        }
                    }
                }
            }
        }
        return result;
    }

    private static boolean calRate(SampleRate sampleRate, ProcessorContext context, double randomDouble) {
        Stream<Boolean> stream = Stream.of(
            checkIp(sampleRate),
            checkDuration(sampleRate),
            checkPeriod(sampleRate),
            checkUtdid(sampleRate, context),
            checkProcessor(sampleRate, context),
            checkRate(sampleRate, context, randomDouble),
            !MathUtil.equal(sampleRate.rate, 0.0D) && MathUtil.moreThan(sampleRate.rate, randomDouble)
        ).filter(Objects::nonNull);
        if (sampleRate.or) {
            return stream.filter(t -> t).findFirst().orElse(false);
        } else {
            return stream.allMatch(t -> t);
        }
    }

    public static Boolean isSampleWriteToTunel(ProcessorContext context) {
        Boolean isSample = ThreadLocalUtils.getTPP_DEBUG_SAMPLE().get();
        if (isSample == null) {
            JSONObject value = JsonUtils.getParam(TppConfigUtil.getJson(SolutionConfig.SOLUTION_CONFIG), SolutionConfig.DEBUG_SAMPLE);
            if (JsonUtils.getParam(value, IS_OPEN, false)) {
                Context ctx = context.getTppContext();
                String cacheKey = ctx.getCurrentAppId() + StringPool.DASH + ctx.getCurrentAbId();
                SampleRate sampleRate = SAMPLE_CACHE.getUnchecked(cacheKey);
                int hashcode = value.hashCode();
                if (hashcode != sampleRate.hashcode) {
                    initSampleRate(value, (sampleRate = new SampleRate()).setHashcode(hashcode));
                    SAMPLE_CACHE.put(cacheKey, sampleRate);
                }
                double randomDouble = ThreadLocalRandom.current().nextDouble();
                isSample = calRate(sampleRate, context, randomDouble);
            } else {
                isSample = false;
            }
            ThreadLocalUtils.getTPP_DEBUG_SAMPLE().set(isSample);
            return isSample;
        }
        return isSample;
    }

    private static void createFuncs(JSONObject jsonObject, SampleRate sampleRate) {
        JSONObject processor = JsonUtils.getParam(jsonObject, TYPE_PROCESSOR, NULL_JSON_OBJECT);
        String clazz = StringUtils.trimToNull(processor.getString(CLASS));
        if (StringUtils.isNotEmpty(clazz)) {
            Processor p = TppObjectFactory.getBean(clazz, null, JsonUtils.getParam(processor, CONFIG, NULL_JSON_OBJECT), Processor.class);
            if (p instanceof IDebugDataSample) {
                sampleRate.processor = FuncUtils.createProcessorFunc(() -> (IDebugDataSample) p);
            }
        }
        List<Pair<FuncType, List<Function<Object, Boolean>>>> result = Lists.newArrayList();
        List<Map<Object, Double>> rateList = Lists.newArrayList();
        JSONArray config = JsonUtils.getParam(jsonObject, CONFIG, NULL_JSON_ARRAY);
        for (Object json : config) {
            if (json instanceof JSONObject) {
                for (String key : ((JSONObject) json).keySet()) {
                    List<Pair<FuncType, Function>> list = FuncUtils.createFunc(key);
                    if (list.size() == 1) {
                        Map map = Maps.transformValues(JsonUtils.getParam((JSONObject) json, key, NULL_JSON_OBJECT),
                            StrUtils::objToDouble);
                        if (!map.isEmpty()) {
                            rateList.add(map);
                            result.addAll((List) list.stream().map(e -> ImmutablePair.of(e.getLeft(), Collections.singletonList(e.getRight()))).collect(Collectors.toList()));
                        }
                    }
                }
            }
        }
        sampleRate.sampleList = result.isEmpty() ? Collections.emptyList() : result;
        sampleRate.rateList = rateList.isEmpty() ? Collections.emptyList() : rateList;
    }
}
