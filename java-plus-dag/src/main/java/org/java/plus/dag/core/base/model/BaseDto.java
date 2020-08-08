package org.java.plus.dag.core.base.model;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
//import com.taobao.recommendplatform.protocol.recommenddomain2.BaseRecommend;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AlgInfoKeyEnum;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoType;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoValueType;
import org.java.plus.dag.core.base.utils.Debugger;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author seven.wxy
 * @date 2018/10/10
 */
@SuppressWarnings("all")
public class BaseDto implements Serializable, Cloneable {
    private static final long serialVersionUID = 1624900887226964477L;
    private String id;
    private int type;
    private StringBuilder algInfo = new StringBuilder();
    private transient Map<AlgInfoKeyEnum, Object> algInfoMap = Maps.newHashMap();
    private String recext = StringUtils.EMPTY;
    private double score = 0.0;
    private String mId = StringUtils.EMPTY;
    /**
     * �زķ���ͼ��ַ
     */
    @SerializedName("m_url")
    private String mUrl;
    /**
     * �زĿ��
     */
    @SerializedName("m_width")
    private String mWidth;
    /**
     * �زĸ߶�
     */
    @SerializedName("m_height")
    private String mHeight;
    /**
     * �زı���
     */
    @SerializedName("m_title")
    private String mTitle;
    /**
     * �ز��ӱ���
     */
    @SerializedName("m_subtitle")
    private String mSubtitle;
    private Map extData = Maps.newHashMap();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getAlgInfo() {
        return algInfo.toString();
    }

    public BaseDto() {

    }

    public BaseDto(String algInfo) {
        this.algInfo.append(StringUtils.trimToEmpty(algInfo));
    }

    public BaseDto(String id, int type) {
        this.id = id;
        this.type = type;
    }

    public BaseDto(String id, int type, double score) {
        this(id, type);
        this.score = score;
    }

    private static boolean allowAppend(AlgInfoKeyEnum key) {
        return key.getType() == AlgInfoType.ON_LINE || (Debugger.isDebug() && key.getType() == AlgInfoType.DEBUG);
    }

    public BaseDto appendAlgInfo(AlgInfoKeyEnum key, Object value) {
        if (allowAppend(key)) {
            if (value != null) {
                if (key.getValueType() == AlgInfoValueType.MULTI) {
                    ((Set) getAlgInfoMap().computeIfAbsent(key, (k) -> Sets.newLinkedHashSet())).add(value);
                } else {
                    getAlgInfoMap().put(key, value);
                }
            }
        }
        return this;
    }

    public void genAlgInfo(String prefix) {
        this.algInfo.append(genAlgInfo(this.getAlgInfoMap(), prefix));
    }

    public static StringBuilder genAlgInfo(Map<AlgInfoKeyEnum, Object> algInfoMap, String prefix) {
        StringBuilder stringBuilder = new StringBuilder();
        if (MapUtils.isNotEmpty(algInfoMap)) {
            NumberFormat nf = NumberFormat.getNumberInstance();
            nf.setMaximumFractionDigits(5);
            nf.setGroupingUsed(false);
            for (Map.Entry entry : algInfoMap.entrySet()) {
                AlgInfoKeyEnum key = (AlgInfoKeyEnum) entry.getKey();
                Object value = entry.getValue();
                if (allowAppend(key)) {
                    if (value != null) {
                        if (key.getValueType() == AlgInfoValueType.MULTI) {
                            value = ((Set) value)
                                .stream()
                                .map(e -> e instanceof Double || e instanceof Float ?
                                    //                                        new BigDecimal((double) e).setScale(5, RoundingMode.UP)
                                    nf.format(e)
                                    : e.toString())
                                .collect(Collectors.joining(StringPool.COMMA));
                        } else {
                            if (value instanceof Double || value instanceof Float) {
                                //                        value = new BigDecimal((double) value).setScale(5, RoundingMode.UP);
                                value = nf.format(value);
                            }
                        }
                        value = StringUtils.replaceChars(value.toString(), StringPool.C_DASH, StringPool.C_PLUS);
                        stringBuilder.append(AlgInfo.key).append(prefix).append(key.getName()).append(AlgInfo.value).append(value);
                    }
                }
            }
        }
        return stringBuilder;
    }

    public String getRecext() {
        return recext;
    }

    public BaseDto setRecext(String recext) {
        this.recext = recext;
        return this;
    }

    public double getScore() {
        return score;
    }

    public BaseDto setScore(double score) {
        this.score = score;
        return this;
    }

    public Map<AlgInfoKeyEnum, Object> getAlgInfoMap() {
        return Objects.isNull(algInfoMap) ? algInfoMap = Maps.newHashMap() : algInfoMap;
    }

    public Map getExtData() {
        return Objects.isNull(extData) ? extData = Maps.newHashMap() : extData;
    }
 
    public String getIdentify() {
        return type + "_" + id + "_" + score + "_" + 0.0;
    }

    @Override
    public String toString() {
        return "BaseDto{" +
            "id='" + id + '\'' +
            ", type=" + type +
            ", recext='" + recext + '\'' +
            ", score=" + score +
            ", mId='" + mId + '\'' +
            ", mUrl='" + mUrl + '\'' +
            ", mWidth='" + mWidth + '\'' +
            ", mHeight='" + mHeight + '\'' +
            ", mTitle='" + mTitle + '\'' +
            ", mSubtitle='" + mSubtitle + '\'' +
            ", extData=" + extData +
            '}';
    }

    public String getmId() {
        return mId;
    }

    public BaseDto setmId(String mId) {
        this.mId = mId;
        return this;
    }

    public String getmUrl() {
        return mUrl;
    }

    public BaseDto setmUrl(String mUrl) {
        this.mUrl = mUrl;
        return this;
    }

    public String getmWidth() {
        return mWidth;
    }

    public BaseDto setmWidth(String mWidth) {
        this.mWidth = mWidth;
        return this;
    }

    public String getmHeight() {
        return mHeight;
    }

    public BaseDto setmHeight(String mHeight) {
        this.mHeight = mHeight;
        return this;
    }

    public String getmTitle() {
        return mTitle;
    }

    public BaseDto setmTitle(String mTitle) {
        this.mTitle = mTitle;
        return this;
    }

    public String getmSubtitle() {
        return mSubtitle;
    }

    public BaseDto setmSubtitle(String mSubtitle) {
        this.mSubtitle = mSubtitle;
        return this;
    }
}
