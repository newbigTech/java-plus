package org.java.plus.dag.core.base.em;

/**
 * @author seven.wxy
 * @date 2018/11/19
 */
public enum RecallType implements EnumInterface {
    INTEREST_TAG(-11),
    UPLOADER(-12),
    EXPAND_I2I(-13),
    JX_I2I(-10),
    OFFLINE_I2I(-9),
    REALTIME_I2I(-8),
    OFFLINE_SRCH(-7),
    REALTIME_SRCH(-6),
    U2I(-5),
    SWING_I2I(-4),
    SQ_I2I(-3),
    I2I(-2),
    YZ_I2I(-1),
    REALTIME_SEARCH(0),
    OGC_I2I(1),
    TX_TAG_MATCH(3),
    REALTIME_C2I(4),
    C2I(5),
    TAG_MATCH(6),
    REALTIME_TAG_MATCH(7),
    RETARGET(8),
    OWNER2I(9),
    HOT(10),
    REALTIME_C2S2I(11),
    CITY2I(12),
    POOL2I(13),
    CONTENT_I2I(14),
    PERSON_I2I(15),
    SHOWNAME_I2I(16),
    TAG2TAG(17),
    TAG_EXPLORE(18),
    REALTIME_SHOW2I(19),
    SHOW2I(20),
    REALTIME_SHOW2SHOW(21),
    SHOW2SHOW(22),
    CITY2TAG2I(23),
    CHNL2CHNL(24),
    OWNER2OWNER(25),
    TAG_VIDEO_LIST(26),
    COLD_START_ITEM(27),
    COLD_START_TAG(28),
    GRU(29),
    NEW_VDO(30),
    UCCONTENT(31),
    REALTIME_HOT(32),
    STAR2I(33),
    VECTOR_TAG2I(34),
    VERTICAL_I2I(35),
    VERTICAL_TAG2I(36),
    VERTICAL_HOT(37),
    REALTIME_OWNER2I(38),
    HOT_TOPIC(39),
    FIND_I2I(40),
    REALTIME_TAG2I(41),
    I2TAG(42),
    PUBLISHID2I(43),
    HOT_SHOW(44),
    TOPIC2I(45),
    MUSIC2I(46),
    ONLINE_ASSIGN(47),
    WORLD_CUP2I(48),
    REALTIME_WORLD_CUP2I(49),
    SCG2I(50),
    TAG2SCG2I(51),
    JS_HOT(52),
    VECTOR2I(53),
    REDDOT_OWNER(54),
    NEWTOPIC2I(55),
    NEWMUSIC2I(56),
    FID_TAG2I(57),
    PEG(58),
    EXP(59),
    FID_I2I(60),
    PUBLISHIDG2I(61),
    ALGGREAT(62),
    ZHUKEHOT(63),
    X2I(64),
    CLUSTER_HOT2I(65),
    NEW_VDO_EE(66),
    SERIES_S2I(67),
    HOT_OZHOU(68),
    RANDOMX2I(69),
    DEEP_I2I(70),
    T2I(71),
    S2I(72),
    CUR_EMBEDDING_I2I(73),
    FEED_EMBEDDING_I2I(74),
    CUR_IMG_I2I(75),
    FEED_IMG_I2I(76),
    NEW_CONTENT_I2I(77),
    TAGNET2I(78),
    REALTIME_TAGNET2I(79),
    SINGLETAG(80),
    REALTIME_SINGLETAG(81),
    CHILDREN_EDUCATION(82),
    UNKNOWN(1000),
    BACKUP(2000),
    WEIBOHOTQUERY(2019),
    /**
     * ¶ÔÓ¦1.0µÄCOMMON_I2I
     */
    COMMON_I2I(83),
    SHOW_POSITIVE(84),
    SHOW_POSITIVE_I2I(85);


    public int order;

    RecallType(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public static RecallType getMatchTypeById(int id) {
        for (RecallType matchType : RecallType.values()) {
            if (matchType.getOrder() == id) {
                return matchType;
            }
        }
        return I2I;
    }

    @Override
    public int getIdentify() {
        return 31 * this.name().hashCode() + this.order;
    }
}