package org.java.plus.dag.core.base.em;

import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoType;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoValueType;
import lombok.Getter;

/**
 * @author seven.wxy
 * @date 2019/7/16
 */
public enum AlgInfoKey implements AlgInfoKeyEnum {
    /**
     * ��¼������ٻ�����
     */
    RC_TYPE("rcType")

    /**
     * ��¼������ٻر���
     */
    ,
    RC_TABLE("rcTable")

    /**
     * �ٻط������ضϵ�С������5λ
     */
    ,
    RC_SCORE("rcScore")

    /**
     * ReduceModel�������ضϵ�С������5λ
     */
    ,
    RM_SCORE("rmScore")

    /**
     * �ٻش���ID
     */
    ,
    RC_TRIG("rcTrig")

    /**
     * currentId����ID
     */
    ,
    CR_TRIG("crTrig")

    /**
     * fid����ID
     */
    ,
    FD_TRIG("fdTrig")

    /**
     * ����ģ������
     */
    ,
    RK_BIZ("rkBiz")

    /**
     * ����ģ������
     */
    ,
    RK_BIZ2("rkBiz2")

    /**
     * ����ģ������
     */
    ,
    RK_BIZ3("rkBiz3")

    /**
     * ����ģ������
     */
    ,
    RK_BIZ4("rkBiz4")

    /**
     * ����ģ������
     */
    ,
    RK_BIZ5("rkBiz5")

    /**
     * ����������ضϵ�С������5λ
     */
    ,
    RK_SCORE("rkScore")
    /**
     * ����������ضϵ�С������5λ
     */
    ,
    RK_TS_SCORE("rkTsScore")
    /**
     * ����ģ������
     */
    ,
    RK_TS_BIZ("rkTsBiz")
    /**
     * ����������ضϵ�С������5λ
     */
    ,
    RK_PCR_SCORE("rkPcrScore")
    /**
     * ����ģ������
     */
    ,
    RK_PCR_BIZ("rkPcrBiz")
    /**
     * ����������ضϵ�С������5λ
     */
    ,
    RK_CTR_SCORE("rkCtrScore")
    /**
     * ����ģ������
     */
    ,
    RK_CTR_BIZ("rkCtrBiz")
    /**
     * ����������ضϵ�С������5λ
     */
    ,
    RK_LTR_SCORE("rkLtrScore")
    /**
     * ����ģ������
     */
    ,
    RK_LTR_BIZ("rkLtrBiz")
    /**
     * ��ɢ�������ضϵ�С������5λ
     */
    ,
    RK_SCT_SCORE("rkSctScore")
    /**
     * ����������ضϵ�С������5λ
     */
    ,
    RK_FB_SCORE("rkFbScore")
    /**
     * ����ģ������
     */
    ,
    RK_FB_BIZ("rkFbBiz")

    /**
     * ģ��2����������ضϵ�С������5λ
     */
    ,
    RK_SCORE2("rkScore2")

    /**
     * ģ��3����������ضϵ�С������5λ
     */
    ,
    RK_SCORE3("rkScore3")

    /**
     * ģ��4����������ضϵ�С������5λ
     */
    ,
    RK_SCORE4("rkScore4")

    /**
     * ģ��5����������ضϵ�С������5λ
     */
    ,
    RK_SCORE5("rkScore5")

    /**
     * ģ�;�ϲ������������ضϵ�С������5λ
     */
    ,
    UNEXPECTEDNESS_SCORE("unExpected")

    /**
     * �����������е�λ��
     */
    ,
    RK_RANK("rkRank")

    /**
     * context feature
     */
    ,
    CONTEXT_FEATURE("ctxFea")

    /**
     * downbeauty֮��ķ���
     */
    ,
    DB_SCORE("dbScore")

    /**
     * ʵʱ����֮��ķ���
     */
    ,
    RT_SCORE("rtScore")

    /**
     * ��¼������������ٻر���
     */
    ,
    COLD_TYPE("coldType")

    /**
     * �������ٻط������ضϵ�С������5λ
     */
    ,
    COLD_SCORE("coldScore")

    /**
     * �������ٻش���ID
     */
    ,
    COLD_TRIG("coldTrig")

    /**
     * �������
     */
    ,
    PAGE_TYPE("pagetype")

    /**
     * ��¼pn
     */
    ,
    PN("pn")

    /**
     * ��¼adsPageNo
     */
    ,
    ADS_PAGE_NO("adsPageNo")

    /**
     * ���������Ȩ
     */
    ,
    ADDWHT_TYPE("addwht"),
    MINUSWHT_TYPE("minuswht")

    /**
     * ��ʶ�Ƿ񾭹��������Ĵ�ɢ�߼�
     */
    ,
    SCATTER_FLAG("scatterFlag")

    /**
     * ǿ��
     */
    ,
    INSERT("insert")

    /**
     * �زĸ��Ի����
     */
    ,
    META_MATCH_ID("meta_match_id")

    /**
     * �زĸ��Ի�mid���
     */
    ,
    MID("mid")

    /**
     * trigger����ʱ��͵�ǰʱ��Ĳ�ֵ,�뼶��
     */
    ,
    RC_TRIG_TIME("recallTrigTime")

    /**
     * trigger����ʱ��
     */
    ,
    RC_TRIG_TS("recallTrigTs")

    /**
     * Item���ٻ�trigger�����е�����,������1��ʼ
     */
    ,
    RC_RN("recallRn")

    /**
     * ��¼������ٻ�����
     */
    ,
    RC_BE_TYPE("rcBeType")

    /**
     * ��¼�ٻص�trigger����
     */
    ,
    RC_TRIG_NUM("rcTrigNum")

    /**
     * ��¼�ٻص�triggerList
     */
    ,
    RC_TRIG_LIST("rcTrigList", AlgInfoValueType.MULTI)

    /**
     * ��¼�ٻص�matchTypeList
     */
    ,
    RC_BETYPE_LIST("rcBeTypeList")

    /**
     * ��¼�ٻص�rcMergeType���������ǰ��·�ٻ��ںϵķ�ʽ
     */
    ,
    RC_MERGE_TYPE("rcMergeType")

    /**
     * ��¼�ٻص�rcReRankType�����ǿ��͸�����ٻط�ʽ
     */
    ,
    RC_RERANK_TYPE("rcReRankType")

    /**
     * tpp scene id
     */
    ,
    SCENE_ID("sceneId")

    /**
     * ��Ͷ�ƻ���Ⱥ����ID
     */
    ,
    PLAN_RULE_ID("planRuleId")

    /**
     * ��Ͷ�ƻ�Ͷ��ID
     */
    ,
    PLAN_PUBLISH_ID("planPublishId")

    /**
     * ��Ͷ�ƻ�����
     */
    ,
    PLAN_TYPE("planType")

    /**
     * ��Ͷ�ƻ�ID
     */
    ,
    PLAN_CONFIG_ID("planConfigId")

    /**
     * ��¼�����������ٻ�˳��
     */
    ,
    PAGE_WISE("pageWise")

    /**
     * ��ɢ����
     */
    ,
    SCATTER_LEN("scatterLen")

    /**
     * tpp abId
     */
    ,
    AB_ID("abId")

    /**
     * tpp abId
     */
    ,
    RatioInfo("ratioInfo")

    /**
     * tpp layer info
     */
    ,
    TPP_BUCKETS("tpp_buckets")

    /**
     * model
     */
    ,
    MODEL("model")

    /**
     * kid flag
     */
    ,
    KID("kid")

    /**
     * Bandit �㷨��ʶ
     */
    ,BANDIT_INFO("banditInfo"), BANDIT_CTX("banditCtx"), BANDIT_ID("banditId")

    /**
     * PGC����Ƶ�������һ�
     */
    ,BID_PRICE("bidPrice"), NET_INCOME("netIncome"), INITIAL_BALANCE("initialBalance")

    /**
     * IDST�㷨������ʶ
     */
    ,
    OLAD_AB_ID("olad_ab_id")

    /**
     * ˫����ʵ�����ID
     */
    ,
    ALG_TITLE_ID("algTitleId"),
    EDC_TAG("edcA"),
    OWNER_ID("ownerId"),
    VDO_LEN("vdoLen"),
    TOP_ID("topId")
    /**
     * ������¼һЩ�Զ����key value
     * ��ʽ��key \u0006 value \u0007 key \u0006 value
     */
    ,
    ALG_KEY("algKey")
    /**
     * trig_ext info
     */
    ,
    TRIG_EXT("trig_ext");

    @Getter
    public String name;
    @Getter
    public AlgInfoType type = AlgInfoType.ON_LINE;
    @Getter
    public AlgInfoValueType valueType = AlgInfoValueType.SINGLE;

    AlgInfoKey(String name) {
        this.name = name;
    }

    AlgInfoKey(String name, AlgInfoValueType valueType) {
        this.name = name;
        this.valueType = valueType;
    }
}

