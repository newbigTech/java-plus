package org.java.plus.dag.core.base.utils.tair;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Lists;
import org.java.plus.dag.core.base.constants.StringPool;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;


public class SessionLdbDataMerge implements ILdbDataMerge {

    private static final SessionLdbDataMerge instance = new SessionLdbDataMerge();

    private SessionLdbDataMerge() {
    }

    public static SessionLdbDataMerge getInstance() {
        return instance;
    }

    @Override
    public Serializable merge(Serializable oldVersionData, Serializable newVersionData) {
        Queue<String> oldSessionQueue = Lists.newLinkedList();
        Queue<String> newSessionQueue = Lists.newLinkedList();

        parseDataToQueue(oldSessionQueue, String.valueOf(oldVersionData));
        parseDataToQueue(newSessionQueue, String.valueOf(newVersionData));

        Set<String> originSet = new HashSet<>(newSessionQueue);
        for (String item : oldSessionQueue) {
            if (!originSet.contains(item)) {
                newSessionQueue.offer(item);
            }
        }
        return parseQueueToString(newSessionQueue);
    }

    private void parseDataToQueue(Queue<String> queue, String data) {
        if (StringUtils.isNotEmpty(data)) {
            String[] arr = StringUtils.splitByWholeSeparator(data, StringPool.COMMA);
            for (String str : arr) {
                queue.offer(str);
            }
        }
    }

    private String parseQueueToString(Queue<String> queue) {
        StringBuilder needPutData = new StringBuilder();
        if (CollectionUtils.isNotEmpty(queue)) {
            queue.forEach(p ->
                    needPutData.append(p).append(StringPool.COMMA)
            );

            if (needPutData.length() > 1 && needPutData.charAt(needPutData.length() - 1) == StringPool.COMMA.charAt(0)) {
                needPutData.setLength(needPutData.length() - 1);
            }
        }
        return needPutData.toString();
    }
}
