package org.java.plus.dag.core.base.utils;

import com.google.common.collect.Lists;
import org.java.plus.dag.core.base.model.ProcessorContext;

import java.util.*;

public class ScatterUtils {
    /**
     * ���ش�ɢ���,������֤���ڵ�Ԫ�����Բ�ͬ���ٻ�
     *
     * @param context
     * @param ratioMap:�ٻ�����  -> ��������ռ����ı���
     * @param forcePosMap:ǿ�� ǿ��λ�� -> �ٻ�����
     * @param oriCount:      ��������(��ʵ�ķ�������  ��Ҫȥ��ǿ��֮�������)
     * @return
     */
    public static List<String> scatterStrategy(ProcessorContext context, Map<String, Double> ratioMap, Map<Integer, String> forcePosMap, int oriCount) {
        //�ٻؽ������->�ٻ�����(��ͬ�����ļ�¼��ͬһ��List����)
        TreeMap<Integer, List<String>> order = new TreeMap<>(Comparator.reverseOrder());
        //�ٻؽ������
        TreeSet<Integer> numbers = new TreeSet<>();
        //ά��������Щ�ٻ�����
        Set<String> elements = new HashSet<>();
        int newCount = 0;
        Map<String, Integer> forceKey2Num = new HashMap<>();
        //����ÿ���ٻ�ǿ�������
        forcePosMap.forEach((k, v) -> {
            int val = forceKey2Num.getOrDefault(v, 0);
            forceKey2Num.put(v, val + 1);
        });
        int totalForceNum = 0;
        for (Map.Entry<String, Double> me : ratioMap.entrySet()) {
            //����ÿ·�ٻؽ����Ҫ��ӵ�����,���ͳһ��1,ͬʱȥ��ǿ�������
            int forceNum = forceKey2Num.getOrDefault(me.getKey(), 0);
            int cnt = (int) Math.ceil(me.getValue() * oriCount) - forceNum;
            totalForceNum += forceNum;
            newCount += cnt;
            order.computeIfAbsent(cnt, (key) -> Lists.newLinkedList()).add(me.getKey());
            elements.add(me.getKey());
            numbers.add(cnt);
        }
        //���ص�������Ҫȥ��ǿ�������
        oriCount -= totalForceNum;
        List<String> list = new ArrayList<>();
        int lastValue = 0;
        int num = 0;
        while (!numbers.isEmpty()) {
            int first = numbers.first();
            //ÿ�ν�ʣ������������� �ٻ����� ������
            //���� A:5  B:3  C:2 �γ� ABCABC���� ==> A:3 B:1
            num = first - lastValue;
            //����ֻʣһ������,������������ֻ��һ���ٻ�����,��Ҫ�������ٻ����͵Ľ�����뵽�����ڵ�λ��
            if (numbers.size() == 1 && elements.size() == 1) {
                //��Ϊ����������ʱ��ͳһ��1,������ʱ�����������ظ���
                num -= (newCount - oriCount);
                //�õ������ٻ�����
                String ele = order.get(first).get(0);
                //������Ȼ��Ҫ׷�ӵ�Ԫ��,�����ٻ�������2������,��ô���ȳ��Բ��������ٻ�������֮����ͬ��λ��
                if (num > 0 && ratioMap.size() > 2) {
                    List<String> tmpList = new ArrayList<>(list.size() + num);
                    //���� i,i+1λ�õ��ٻ�������֮����ƥ��,��ô���뵽i+1λ��
                    int i = 0;
                    for (; i < list.size() - 1 && num > 0; i++) {
                        tmpList.add(list.get(i));
                        if (Objects.equals(ele, list.get(i))) {
                            continue;
                        }
                        if (Objects.equals(ele, list.get(i + 1))) {
                            tmpList.add(list.get(i + 1));
                            i++;
                            continue;
                        }
                        tmpList.add(ele);
                        num--;
                    }
                    //����num�м��Ϊ0,��Ҫ��û����ӵ�append����
                    if (i < list.size()) {
                        tmpList.addAll(list.subList(i, list.size()));
                    }
                    list = tmpList;
                }
                if (num > 0) {
                    //���绹��ʣ��,��ô�����iλ�õ��ٻ�������֮����ƥ��,�ŵ���i��λ��
                    List<String> tmpList = new ArrayList<>(list.size() + num);
                    int i = 0;
                    for (; i < list.size() && num > 0; i++) {
                        tmpList.add(list.get(i));
                        if (Objects.equals(list.get(i), ele)) {
                            continue;
                        }
                        tmpList.add(ele);
                        num--;
                    }
                    //����num�м��Ϊ0,��Ҫ��û����ӵ�append����
                    if (i < list.size()) {
                        tmpList.addAll(list.subList(i, list.size()));
                    }
                    list = tmpList;
                }
                //���绹��ʣ��,��ô�����뵽���
                while (num-- > 0) {
                    list.add(ele);
                }

            }
            //ÿ�ν���ǰ�������ٻ����������
            for (int i = 0; i < num; i++) {
                for (String ele : elements) {
                    list.add(ele);
                }
            }
            //ɾ������ǰ������
            numbers.remove(first);
            //ɾ������ǰ������Ԫ��
            elements.removeAll(order.get(first));
            //���µ�ǰ���ѵ�Ԫ������
            lastValue = first;
        }
        List<String> res = list;
        if (res.size() > oriCount) {
            res = res.subList(0, oriCount);
        }

        return res;
    }

    /**
     * ���ֱ������д�ɢ,��������ΪoriCount - ǿ������
     *
     * @param context
     * @param ratioMap:�ٻ�����       ->����
     * @param forcePosMap:ǿ��λ��    ->�ٻ�����
     * @param oriCount:ԭʼ��Ҫ��count
     * @return
     */
    public static List<String> scatterStrategyByRandom(ProcessorContext context, Map<String, Double> ratioMap, Map<Integer, String> forcePosMap, int oriCount) {
        Map<String, Integer> type2Num = new HashMap<>();
        Map<String, Integer> forceKey2Num = new HashMap<>();
        //����ÿ���ٻ�ǿ�������
        forcePosMap.forEach((k, v) -> {
            int val = forceKey2Num.getOrDefault(v, 0);
            forceKey2Num.put(v, val + 1);
        });
        int totalForceNum = 0;
        for (Map.Entry<String, Double> me : ratioMap.entrySet()) {
            //����ÿ·�ٻؽ����Ҫ��ӵ�����,���ͳһ��1,ͬʱȥ��ǿ�������
            int forceNum = forceKey2Num.getOrDefault(me.getKey(), 0);
            int cnt = (int) Math.ceil(me.getValue() * oriCount) - forceNum;
            totalForceNum += forceNum;
            type2Num.put(me.getKey(), cnt);
        }
        oriCount -= totalForceNum;
        int randomMax = oriCount;
        List<String> result = new ArrayList<>(oriCount);
        for (int i = 0; i < oriCount; i++) {
            //�������һ�� [0,randomMax)��ֵ(randomMaxΪ��ǰʣ�����Ҫ��ӵ�����)
            int rval = RandomUtils.nextInt(randomMax);
            //����rval�䵽�ĸ�����,��ǰ����һ���ٻ����͵Ľ��
            int curVal = 0;
            for (Map.Entry<String, Integer> me : type2Num.entrySet()) {
                curVal += me.getValue();
                if (rval < curVal) {
                    result.add(me.getKey());
                    randomMax--;
                    //��ǰ�ٻ����͵����� -1,�ϸ�֤��������
                    type2Num.put(me.getKey(), me.getValue() - 1);
                    break;
                }
            }
        }
        return result;
    }
}
