package org.java.plus.dag.core.base.em;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author youku
 */
public enum AllFieldName implements FieldNameEnum {
	// Recommend Result
	id(String.class), type(Integer.class), score(Double.class), triggerId(String.class), algInfo(String.class),
	recallType(RecallType.class), recext(String.class), extData(Map.class), algInfoMap(Map.class),
	tagNameIdMap(Map.class), item_list(String.class), extMap(Map.class),

	// Video meta
	vdo_title(String.class), vdo_len(Double.class), vdo_type(String.class), show_name(String.class),
	vdo_size(String.class), vdo_format(String.class), subcategory_ids(String.class), owner_name(String.class),
	src_begin(String.class), src_vid(Long.class), owner_id(Long.class), original_owner_name(String.class),
	show_id(Long.class), show_id_i(String.class), show_id_j(String.class), category_id(Long.class),
	upgc_tags(String.class), ext(String.class), published_time(String.class), theme(String.class),
	publish_type(String.class), comefrom(String.class), edit_cuturl(String.class), edit_title(String.class),
	virtual_category_id(String.class), ext1(String.class), exp_tags(String.class), ext2(String.class),
	src_end(String.class), show_vdo_stage(Long.class), chnId(Long.class), tagList(Set.class), title(String.class),
	len(Double.class), expCtr(String.class), source(List.class), disu(Integer.class), showcategory(String.class),
	related_tag(String.class), vdo_tags(String.class), topic_id(Long.class), music_id(Long.class),
	safe_level(Long.class), stat(Long.class), state(Long.class), system_rating(String.class),
	artificial_rating(String.class), vdo_status(String.class), tagIdNameMap(Map.class), expTags(String.class),
	long_video_id(Long.class), parent_id(String.class), douban_score(String.class), // �����, ����item�����ֿɸ���

	// Video extmeta
	first_category_id(String.class), second_category_id(String.class), first_category_name(String.class),
	second_category_name(String.class), category_name(String.class), level(Integer.class), ling_lang_cate(String.class),

	// category_id(String.class),

	// tag
	tag_id(Long.class), tag_list(String.class), tag_name(String.class), tag_category(String.class), name(String.class),
	tag_weight(Double.class), free_tags(String.class), fix_tags(String.class), trgtag2tag(String.class),
	user_realtime_tag(String.class),

	// BE
	vdo_id_b(String.class), result(String.class), vdo_id_a(String.class), match_type(Integer.class),
	weight(Double.class), __score__(Double.class), publishid(String.class), itemid(String.class),
	trigger_num(Long.class), match_type_list(String.class), triggerItem(String.class), recallScore(Double.class),
	triggerItemStr(String.class), alpha(Float.class), beta(Float.class),

	// PlayLog
	vdo_id(String.class), ts(Double.class), vst_time(Long.class),

	// Related recommend
	vdo_chnl_id(String.class), vdo_list(String.class), vid(Long.class), orgvid(Long.class), dvid(Long.class),
	tokenJaccardSim(Double.class), tokenCosineSim(Double.class), algTagJaccardSim(Double.class),
	algTagCosineSim(Double.class), uploaderTagJaccardSim(Double.class), uploaderTagCosineSim(Double.class),
	triggerItemId(Long.class), gapTime(Long.class), recallPos(Long.class), recallCore(Double.class), svid(String.class),
	timeStamp(Long.class), dvidList(List.class), center(String.class),

	// Tair common
	tair_key(String.class), tair_value(Serializable.class), tair_version(Integer.class),

	// IGraph common
	igraph_pkey(String.class), igraph_skey(String.class), igraph_common_value(String.class),
	igraph_filter(String.class), igraph_orderby(String.class), igraph_range(Integer.class), expose_15m(String.class),
	vv_15m(String.class), ts_15m(String.class), expose_30m(String.class), vv_30m(String.class), ts_30m(String.class),
	expose_1h(String.class), vv_1h(String.class), ts_1h(String.class), expose_6h(String.class), vv_6h(String.class),
	ts_6h(String.class), expose_12h(String.class), vv_12h(String.class), ts_12h(String.class), expose_24h(String.class),
	vv_24h(String.class), ts_24h(String.class), vector(String.class),

	// discovery
	playComplete(Double.class), vv(Double.class), averageTs(Double.class), time(String.class), centroid(String.class),
	unexpectedness(Double.class),

	// Session
	items(String.class), count(Integer.class), mask(Integer.class),

	// ABFS
	ABFSFeatureKey(Map.class), ABFSFeatureName(String.class), QinfoMap(Map.class),
//    features(com.taobao.recommendplatform.protocol.domain.abfs.AbfsPersonalizerResult.class),

	// tag display
	json_param(String.class), arg1(String.class), arg2(String.class), eeScore(Double.class), prior_level(Long.class),
	total_cnt(Long.class), tag_class(String.class), seq_id(Long.class), ab_id(Long.class), human_force(Long.class),
	human_prior(Long.class), show_cnt(Long.class), vdo_cnt(Long.class), tag_source(String.class),
	cate_name(String.class),

	// RTP
	fea_json(String.class), feature_json(JSONObject.class), item_id(String.class),

	// alg common data ext info
	model(String.class),

	// DataSet join hidden column
	rightExists(Boolean.class),

	// be recall
	utdid(String.class), uid(String.class), rn(Integer.class), pn(Integer.class), show_type(String.class),
	time_stamp(Long.class), search_word(String.class), item_type(String.class), starid(String.class),
	server_time(String.class), query(String.class), betriggerKey(String.class), betriggerValue(String.class),
	user_trigger_vid(String.class),

	// Semantic deduplication
	vdo_id_1(String.class), vdo_id_2_list(String.class),

	// feature related
	expose15m(Double.class), expose05h(Double.class), tag(String.class), zhengpian(String.class),

	// Feedback, list contain FeedbackReason
	feedback_reasons(List.class),

	// split theme list
	themeList(Set.class),
	// split channel
	channel(Long.class),

	// vid similarity
	token(String.class), alg_tag(String.class), uploader_tag(String.class), token_set(Set.class),
	alg_tag_set(Set.class), uploader_tag_set(Set.class), embedding_title_vector(String.class),
	embedding_tag_vector(String.class),

	// DataSource tableName
	ds_source(String.class), ds_query_index(Integer.class),

	// TrafficRegulationRerank
	hour(String.class), exp_uv_tot(String.class), play_uv_tot(String.class),

	behaviorCount(String.class),

	// DoubleTitle
	double_title_exposure0(int.class), double_title_exposure1(int.class), double_title_exposure2(int.class),
	double_title_exposure3(int.class),

	double_title_click0(int.class), double_title_click1(int.class), double_title_click2(int.class),
	double_title_click3(int.class),

	double_title_title0(String.class), double_title_title1(String.class), double_title_title2(String.class),
	double_title_title3(String.class),

	// bandit
	embedding(String.class), exposure(Integer.class), click(Integer.class), play_control(Integer.class),
	bandit_id(String.class),

	income(Double.class), balance(Double.class),

	// user
	user_type(Long.class),

	// FeedItem
	matchType(RecallType.class), itemId(Long.class), vdo_set(Set.class),

	// main daokan page
	pkey(String.class), scene_id(String.class), headline(String.class), subtitle(String.class),
	channel_names(String.class), startags(String.class), themes(String.class), tags(String.class),

	// multi rank score
	score2(Double.class), score3(Double.class), score4(Double.class), score5(Double.class),

	// debug processorNode
	parentIds(List.class), condition(String.class), nodeType(NodeType.class),

	// main page channel tab
	channelkey(String.class), spm(String.class), scg_id(String.class), chnltab_id(String.class),
	chnltab_str(String.class),

	// uc ad experiment
	user_level(Integer.class),

	// push
	msg_source(String.class), resident_city(String.class), click_time(Long.class),

	// scatter
	topic(String.class), scatter_types(Set.class),

	// scg or baoluo igraph
	status(String.class), publish_id(String.class), star_persons(String.class), scg_type(String.class),
	tag_names(String.class), data_type(String.class), item_ids(String[].class), order_type(String.class),
	source_ids(String.class), scene_ids(String.class), source_type(String.class), belong_id(String.class),
	belong_type(String.class), version_code(String.class), sametag(String.class),

	serial_id(String.class), derived_id(String.class),

	// material(picture + reason)
	mId(String.class), m_url(String.class), m_width(String.class), m_height(String.class), m_title(String.class),
	m_subtitle(String.class), scg_score(Double.class),

	show_idx(Integer.class),

	// item shop info
	shop_categories_id_lists(String.class), shop_id(String.class),

	city_id(String.class), city_name(String.class),

	// dummy,prepare for igraph config
	dummy(Object.class),

	// coolheadline
	svt_prefix(String.class),

	// rerankmerge
	rec_category(String.class), merge_rerank_type(String.class);

	@Getter
	@Setter
	private FieldType fieldType;
	@Getter
	@Setter
	private Class<?> clazz;

	AllFieldName(@NonNull Class<?> clazz) {
		initClassAndFieldType(clazz);
	}
}
