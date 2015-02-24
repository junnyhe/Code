import csv
import gzip

# change delimiter to "|"

def convert_data(infile_name,outfile_name):
    # remove space in header, and strange characters in data
    infile=gzip.open(infile_name,'rb')
    incsv=csv.reader(infile)
    
    
    outfile=open(outfile_name,'w')
    outcsv=csv.writer(outfile,delimiter='|')
    
    header=incsv.next()
    header=[var.replace(" ","_").replace("-",".")  for var in header]
    outcsv.writerow(header)
    
    for row in incsv:
        outcsv.writerow([var.replace('"','').replace("'","").replace('\n','').replace('\r','').replace('#','apt') for var in row])
    
    outfile.close()


infile_name='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_ins_imp_woe.csv.gz'
outfile_name='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_ins_imp_woe.csv'

convert_data(infile_name,outfile_name)


infile_name='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_oos_imp_woe.csv.gz'
outfile_name='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_oos_imp_woe.csv'

convert_data(infile_name,outfile_name)




def process_var_lsit(infile_name,outfile_name):
    # remove space in variable names
    infile=open(infile_name,'rU')
    incsv=csv.reader(infile)
    
    
    outfile=open(outfile_name,'w')
    outcsv=csv.writer(outfile)
    
    
    for row in incsv:
        outcsv.writerow([var.replace(" ","_").replace("-",".")  for var in row])
    
    outfile.close()
    
infile_name='/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/model_var_list_signal_tmxboth.csv'
outfile_name='/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/model_var_list_signal_tmxboth_nospace.csv'

process_var_lsit(infile_name,outfile_name)




['payment_request_id', 'state', 'create_time', 'fs_payment_request_id', 'signal_1', 'signal_2', 'signal_4', 'signal_8', 'signal_9', 'signal_10', 'signal_11', 'signal_12', 'signal_13', 'signal_14', 'signal_15', 'signal_16', 'signal_17', 'signal_18', 'signal_19', 'signal_24', 'signal_25', 'signal_26', 'signal_27', 'signal_28', 'signal_29', 'signal_31', 'signal_33', 'signal_34', 'signal_35', 'signal_36', 'signal_37', 'signal_38', 'signal_39', 'signal_40', 'signal_41', 'signal_42', 'signal_43', 'signal_44', 'signal_45', 'signal_46', 'signal_47', 'signal_48', 'signal_49', 'signal_50', 'signal_58', 'signal_59', 'signal_61', 'signal_62', 'signal_63', 'signal_64', 'signal_65', 'signal_66', 'signal_67', 'signal_68', 'signal_69', 'signal_70', 'signal_71', 'signal_72', 'signal_73', 'signal_74', 'signal_75', 'signal_76', 'signal_77', 'signal_78', 'signal_79', 'signal_127', 'signal_128', 'signal_129', 'signal_140', 'signal_141', 'signal_142', 'signal_143', 'signal_144', 'signal_145', 'signal_146', 'signal_147', 'signal_148', 'signal_149', 'signal_150', 'signal_151', 'signal_152', 'signal_153', 'signal_154', 'signal_155', 'signal_156', 'signal_157', 'signal_158', 'signal_159', 'signal_160', 'signal_161', 'signal_162', 'signal_163', 'signal_164', 'signal_165', 'signal_166', 'signal_167', 'signal_168', 'signal_169', 'signal_170', 'signal_173', 'signal_174', 'signal_175', 'signal_176', 'signal_177', 'signal_178', 'signal_179', 'signal_180', 'signal_181', 'signal_182', 'signal_204', 'signal_228', 'signal_247', 'signal_248', 'signal_300', 'signal_301', 'signal_302', 'signal_303', 'signal_304', 'signal_305', 'signal_306', 'signal_307', 'signal_312', 'signal_313', 'signal_351', 'signal_352', 'signal_353', 'signal_354', 'signal_355', 'signal_356', 'signal_361', 'signal_362', 'signal_371', 'signal_400', 'signal_401', 'signal_402', 'signal_403', 'signal_404', 'signal_405', 'signal_406', 'signal_407', 'signal_408', 'signal_409', 'signal_410', 'signal_411', 'signal_412', 'signal_413', 'signal_414', 'signal_415', 'signal_416', 'signal_417', 'signal_418', 'signal_419', 'signal_420', 'signal_421', 'signal_422', 'signal_423', 'signal_424', 'signal_425', 'signal_426', 'signal_427', 'signal_428', 'signal_429', 'signal_500', 'signal_501', 'signal_503', 'signal_504', 'signal_505', 'signal_506', 'signal_507', 'signal_508', 'signal_509', 'signal_510', 'signal_511', 'signal_512', 'signal_513', 'signal_514', 'signal_515', 'signal_516', 'signal_517', 'signal_518', 'signal_519', 'signal_520', 'signal_521', 'signal_522', 'signal_523', 'signal_524', 'signal_525', 'signal_526', 'signal_527', 'signal_528', 'signal_529', 'signal_530', 'signal_531', 'signal_532', 'signal_533', 'signal_534', 'signal_535', 'signal_536', 'signal_537', 'signal_538', 'signal_539', 'signal_540', 'signal_541', 'signal_542', 'signal_543', 'signal_544', 'signal_545', 'signal_546', 'signal_547', 'signal_548', 'signal_560', 'signal_561', 'signal_570', 'signal_571', 'signal_580', 'signal_590', 'signal_591', 'signal_592', 'signal_593', 'signal_600', 'signal_601', 'signal_602', 'signal_603', 'signal_604', 'signal_605', 'signal_606', 'signal_607', 'signal_608', 'signal_611', 'signal_612', 'signal_613', 'signal_614', 'signal_615', 'signal_616', 'signal_617', 'signal_618', 'signal_100018', 'signal_100024', 'signal_100030', 'signal_100039', 'signal_100042', 'signal_100048', 'signal_100057', 'signal_100066', 'signal_100072', 'signal_100073', 'signal_100083', 'signal_100086', 'signal_100087', 'signal_100096', 'signal_100099', 'signal_100102', 'signal_100108', 'signal_100110', 'tmx_payer_account_address_assert_history', 'tmx_payer_account_address_city', 'tmx_payer_account_address_country', 'tmx_payer_account_address_first_seen', 'tmx_payer_account_address_last_event', 'tmx_payer_account_address_last_update', 'tmx_payer_account_address_result', 'tmx_payer_account_address_score', 'tmx_payer_account_address_state', 'tmx_payer_account_address_street1', 'tmx_payer_account_address_worst_score', 'tmx_payer_account_address_zip', 'tmx_payer_account_email', 'tmx_payer_account_email_activities', 'tmx_payer_account_email_assert_history', 'tmx_payer_account_email_attributes', 'tmx_payer_account_email_first_seen', 'tmx_payer_account_email_last_assertion', 'tmx_payer_account_email_last_event', 'tmx_payer_account_email_last_update', 'tmx_payer_account_email_result', 'tmx_payer_account_email_score', 'tmx_payer_account_email_worst_score', 'tmx_payer_account_login', 'tmx_payer_account_login_assert_history', 'tmx_payer_account_login_first_seen', 'tmx_payer_account_login_last_event', 'tmx_payer_account_login_last_update', 'tmx_payer_account_login_result', 'tmx_payer_account_login_score', 'tmx_payer_account_login_worst_score', 'tmx_payer_account_name', 'tmx_payer_account_name_activities', 'tmx_payer_account_name_assert_history', 'tmx_payer_account_name_attributes', 'tmx_payer_account_name_first_seen', 'tmx_payer_account_name_last_assertion', 'tmx_payer_account_name_last_event', 'tmx_payer_account_name_last_update', 'tmx_payer_account_name_result', 'tmx_payer_account_name_score', 'tmx_payer_account_name_worst_score', 'tmx_payer_agent_type', 'tmx_payer_Array', 'tmx_payer_browser_language', 'tmx_payer_browser_language_anomaly', 'tmx_payer_browser_string', 'tmx_payer_browser_string_anomaly', 'tmx_payer_browser_string_hash', 'tmx_payer_browser_string_mismatch', 'tmx_payer_cidr_number', 'tmx_payer_css_image_loaded', 'tmx_payer_custom_count_1', 'tmx_payer_custom_count_2', 'tmx_payer_custom_match_1', 'tmx_payer_custom_match_2', 'tmx_payer_custom_match_3', 'tmx_payer_custom_policy_score', 'tmx_payer_detected_fl', 'tmx_payer_device_activities', 'tmx_payer_device_assert_history', 'tmx_payer_device_attributes', 'tmx_payer_device_first_seen', 'tmx_payer_device_id', 'tmx_payer_device_id_confidence', 'tmx_payer_device_last_assertion', 'tmx_payer_device_last_event', 'tmx_payer_device_last_update', 'tmx_payer_device_match_result', 'tmx_payer_device_result', 'tmx_payer_device_score', 'tmx_payer_device_worst_score', 'tmx_payer_dns_ip', 'tmx_payer_dns_ip_city', 'tmx_payer_dns_ip_geo', 'tmx_payer_dns_ip_isp', 'tmx_payer_dns_ip_latitude', 'tmx_payer_dns_ip_longitude', 'tmx_payer_dns_ip_organization', 'tmx_payer_dns_ip_region', 'tmx_payer_enabled_ck', 'tmx_payer_enabled_fl', 'tmx_payer_enabled_im', 'tmx_payer_enabled_js', 'tmx_payer_error_detail', 'tmx_payer_event_type', 'tmx_payer_flash_anomaly', 'tmx_payer_flash_lang', 'tmx_payer_flash_os', 'tmx_payer_flash_system_state', 'tmx_payer_flash_version', 'tmx_payer_fuzzy_device_activities', 'tmx_payer_fuzzy_device_first_seen', 'tmx_payer_fuzzy_device_id', 'tmx_payer_fuzzy_device_id_confidence', 'tmx_payer_fuzzy_device_last_event', 'tmx_payer_fuzzy_device_last_update', 'tmx_payer_fuzzy_device_match_result', 'tmx_payer_fuzzy_device_result', 'tmx_payer_fuzzy_device_score', 'tmx_payer_fuzzy_device_worst_score', 'tmx_payer_headers_name_value_hash', 'tmx_payer_headers_order_string_hash', 'tmx_payer_honeypot_fingerprint', 'tmx_payer_honeypot_fingerprint_check', 'tmx_payer_honeypot_fingerprint_diff', 'tmx_payer_honeypot_fingerprint_match', 'tmx_payer_honeypot_unknown_diff', 'tmx_payer_http_os_signature', 'tmx_payer_http_referer', 'tmx_payer_http_referer_domain', 'tmx_payer_http_referer_domain_assert_history', 'tmx_payer_http_referer_domain_first_seen', 'tmx_payer_http_referer_domain_last_event', 'tmx_payer_http_referer_domain_last_update', 'tmx_payer_http_referer_domain_result', 'tmx_payer_http_referer_domain_score', 'tmx_payer_http_referer_domain_worst_score', 'tmx_payer_http_referer_url', 'tmx_payer_image_anomaly', 'tmx_payer_image_loaded', 'tmx_payer_js_browser_string', 'tmx_payer_js_fonts_hash', 'tmx_payer_js_fonts_number', 'tmx_payer_js_href_domain', 'tmx_payer_mime_type_hash', 'tmx_payer_mime_type_number', 'tmx_payer_multiple_session_id', 'tmx_payer_org_id', 'tmx_payer_os', 'tmx_payer_os_anomaly', 'tmx_payer_os_fonts_hash', 'tmx_payer_os_fonts_number', 'tmx_payer_page_time_on', 'tmx_payer_plugin_adobe_acrobat', 'tmx_payer_plugin_devalvr', 'tmx_payer_plugin_flash', 'tmx_payer_plugin_hash', 'tmx_payer_plugin_java', 'tmx_payer_plugin_number', 'tmx_payer_plugin_quicktime', 'tmx_payer_plugin_realplayer', 'tmx_payer_plugin_shockwave', 'tmx_payer_plugin_silverlight', 'tmx_payer_plugin_svg_viewer', 'tmx_payer_plugin_vlc_player', 'tmx_payer_plugin_windows_media_player', 'tmx_payer_policy', 'tmx_payer_policy_score', 'tmx_payer_profiled_domain', 'tmx_payer_profiled_domain_first_seen', 'tmx_payer_profiled_domain_last_event', 'tmx_payer_profiled_domain_last_update', 'tmx_payer_profiled_domain_result', 'tmx_payer_profiled_domain_score', 'tmx_payer_profiled_domain_worst_score', 'tmx_payer_profiled_url', 'tmx_payer_profiling_datetime', 'tmx_payer_proxy_ip', 'tmx_payer_proxy_ip_activities', 'tmx_payer_proxy_ip_assert_history', 'tmx_payer_proxy_ip_attributes', 'tmx_payer_proxy_ip_city', 'tmx_payer_proxy_ip_first_seen', 'tmx_payer_proxy_ip_geo', 'tmx_payer_proxy_ip_isp', 'tmx_payer_proxy_ip_last_assertion', 'tmx_payer_proxy_ip_last_event', 'tmx_payer_proxy_ip_last_update', 'tmx_payer_proxy_ip_latitude', 'tmx_payer_proxy_ip_longitude', 'tmx_payer_proxy_ip_organization', 'tmx_payer_proxy_ip_region', 'tmx_payer_proxy_ip_result', 'tmx_payer_proxy_ip_score', 'tmx_payer_proxy_ip_worst_score', 'tmx_payer_proxy_name', 'tmx_payer_proxy_type', 'tmx_payer_reason_code', 'tmx_payer_request_duration', 'tmx_payer_request_id', 'tmx_payer_request_result', 'tmx_payer_review_status', 'tmx_payer_risk_rating', 'tmx_payer_screen_aspect_ratio_anomaly', 'tmx_payer_screen_color_depth', 'tmx_payer_screen_dpi', 'tmx_payer_screen_res', 'tmx_payer_screen_res_anomaly', 'tmx_payer_service_type', 'tmx_payer_session_anomaly', 'tmx_payer_session_id', 'tmx_payer_summary_risk_score', 'tmx_payer_system_state', 'tmx_payer_tcp_os_signature', 'tmx_payer_timezone_offset_anomaly', 'tmx_payer_time_zone', 'tmx_payer_time_zone_dst_offset', 'tmx_payer_tmx_policy_score', 'tmx_payer_tmx_reason_code', 'tmx_payer_tmx_review_status', 'tmx_payer_tmx_risk_rating', 'tmx_payer_tmx_summary_reason_code', 'tmx_payer_transaction_amount', 'tmx_payer_transaction_currency', 'tmx_payer_transaction_id', 'tmx_payer_true_ip', 'tmx_payer_true_ip_activities', 'tmx_payer_true_ip_assert_history', 'tmx_payer_true_ip_attributes', 'tmx_payer_true_ip_city', 'tmx_payer_true_ip_first_seen', 'tmx_payer_true_ip_geo', 'tmx_payer_true_ip_isp', 'tmx_payer_true_ip_last_assertion', 'tmx_payer_true_ip_last_event', 'tmx_payer_true_ip_last_update', 'tmx_payer_true_ip_latitude', 'tmx_payer_true_ip_longitude', 'tmx_payer_true_ip_organization', 'tmx_payer_true_ip_region', 'tmx_payer_true_ip_result', 'tmx_payer_true_ip_score', 'tmx_payer_true_ip_worst_score', 'tmx_payer_ua_browser', 'tmx_payer_ua_mobile', 'tmx_payer_ua_os', 'tmx_payer_ua_platform', 'tmx_payer_unknown_session', 'tmx_payer_url_anomaly', 'tmx_payer_rs_ind_No reject status on Email - global - month', 'tmx_payer_rs_ind_No reject status on Exact ID - global - month', 'tmx_payer_rs_ind_New Exact ID', 'tmx_payer_rs_ind_Good Exact ID Age', 'tmx_payer_rs_ind_3IPLogins_inaDay', 'tmx_payer_rs_ind_NoDeviceID', 'tmx_payer_rs_ind_Global Email gt 500 dollars - month', 'tmx_payer_rs_ind_Global Exact ID gt 500 dollars - month', 'tmx_payer_rs_ind_3DifferentDeviceIDs_SameAccountEmailID_inaWeek', 'tmx_payer_rs_ind_3DifferentDeviceIDs_SameAccountLogin_inaWeek', 'tmx_payer_rs_ind_3_Emails_per_device', 'tmx_payer_rs_ind_Review Status', 'tmx_payer_rs_ind_Lang Mismatch', 'tmx_payer_rs_ind_Good global persona - month', 'tmx_payer_rs_ind_AccountAddress_Differentfrom_TrueGeo', 'tmx_payer_rs_ind_global email used mlt places -day', 'tmx_payer_rs_ind_ProxyIP_isHidden', 'tmx_payer_rs_ind_ProxyIPAddress_Risky_inReputationNetwork', 'tmx_payer_rs_ind_3DifferentAccountEmailIDs_SameDeviceID_inaDay', 'tmx_payer_rs_ind_3DifferentAccountLogins_SameDeviceID_inaDay', 'tmx_payer_rs_ind_3DifferentAccountEmailIDs_SameDeviceID_inaWeek', 'tmx_payer_rs_ind_3DifferentAccountLogins_SameDeviceID_inaWeek', 'tmx_payer_rs_ind_global device using mlt personas - day', 'tmx_payer_rs_ind_DeviceTrueGEO_Differentfrom_ProxyGeo', 'tmx_payer_rs_ind_Dial-up connection', 'tmx_payer_rs_ind_2Device_Creation_inanHour', 'tmx_payer_rs_ind_ProxyIP_isAnonymous', 'tmx_payer_rs_ind_3DifferentProxyIPs_SameDeviceID_inaDay', 'tmx_payer_rs_ind_PossibleCookieWiping', 'tmx_payer_rs_ind_3DeviceCreation_inaDay', 'merge_key', 'merge_ind', 'tmx_payee_account_address_assert_history', 'tmx_payee_account_address_city', 'tmx_payee_account_address_country', 'tmx_payee_account_address_first_seen', 'tmx_payee_account_address_last_event', 'tmx_payee_account_address_last_update', 'tmx_payee_account_address_result', 'tmx_payee_account_address_score', 'tmx_payee_account_address_state', 'tmx_payee_account_address_street1', 'tmx_payee_account_address_worst_score', 'tmx_payee_account_address_zip', 'tmx_payee_account_email', 'tmx_payee_account_email_activities', 'tmx_payee_account_email_assert_history', 'tmx_payee_account_email_attributes', 'tmx_payee_account_email_first_seen', 'tmx_payee_account_email_last_assertion', 'tmx_payee_account_email_last_event', 'tmx_payee_account_email_last_update', 'tmx_payee_account_email_result', 'tmx_payee_account_email_score', 'tmx_payee_account_email_worst_score', 'tmx_payee_account_login', 'tmx_payee_account_login_assert_history', 'tmx_payee_account_login_first_seen', 'tmx_payee_account_login_last_event', 'tmx_payee_account_login_last_update', 'tmx_payee_account_login_result', 'tmx_payee_account_login_score', 'tmx_payee_account_login_worst_score', 'tmx_payee_account_name', 'tmx_payee_account_name_activities', 'tmx_payee_account_name_assert_history', 'tmx_payee_account_name_attributes', 'tmx_payee_account_name_first_seen', 'tmx_payee_account_name_last_assertion', 'tmx_payee_account_name_last_event', 'tmx_payee_account_name_last_update', 'tmx_payee_account_name_result', 'tmx_payee_account_name_score', 'tmx_payee_account_name_worst_score', 'tmx_payee_agent_type', 'tmx_payee_Array', 'tmx_payee_browser_language', 'tmx_payee_browser_language_anomaly', 'tmx_payee_browser_string', 'tmx_payee_browser_string_anomaly', 'tmx_payee_browser_string_hash', 'tmx_payee_browser_string_mismatch', 'tmx_payee_cidr_number', 'tmx_payee_css_image_loaded', 'tmx_payee_custom_count_1', 'tmx_payee_custom_count_2', 'tmx_payee_custom_match_1', 'tmx_payee_custom_match_2', 'tmx_payee_custom_match_3', 'tmx_payee_custom_policy_score', 'tmx_payee_detected_fl', 'tmx_payee_device_activities', 'tmx_payee_device_assert_history', 'tmx_payee_device_attributes', 'tmx_payee_device_first_seen', 'tmx_payee_device_id', 'tmx_payee_device_id_confidence', 'tmx_payee_device_last_assertion', 'tmx_payee_device_last_event', 'tmx_payee_device_last_update', 'tmx_payee_device_match_result', 'tmx_payee_device_result', 'tmx_payee_device_score', 'tmx_payee_device_worst_score', 'tmx_payee_dns_ip', 'tmx_payee_dns_ip_city', 'tmx_payee_dns_ip_geo', 'tmx_payee_dns_ip_isp', 'tmx_payee_dns_ip_latitude', 'tmx_payee_dns_ip_longitude', 'tmx_payee_dns_ip_organization', 'tmx_payee_dns_ip_region', 'tmx_payee_enabled_ck', 'tmx_payee_enabled_fl', 'tmx_payee_enabled_im', 'tmx_payee_enabled_js', 'tmx_payee_error_detail', 'tmx_payee_event_type', 'tmx_payee_flash_anomaly', 'tmx_payee_flash_lang', 'tmx_payee_flash_os', 'tmx_payee_flash_system_state', 'tmx_payee_flash_version', 'tmx_payee_fuzzy_device_activities', 'tmx_payee_fuzzy_device_first_seen', 'tmx_payee_fuzzy_device_id', 'tmx_payee_fuzzy_device_id_confidence', 'tmx_payee_fuzzy_device_last_event', 'tmx_payee_fuzzy_device_last_update', 'tmx_payee_fuzzy_device_match_result', 'tmx_payee_fuzzy_device_result', 'tmx_payee_fuzzy_device_score', 'tmx_payee_fuzzy_device_worst_score', 'tmx_payee_headers_name_value_hash', 'tmx_payee_headers_order_string_hash', 'tmx_payee_honeypot_fingerprint', 'tmx_payee_honeypot_fingerprint_check', 'tmx_payee_honeypot_fingerprint_diff', 'tmx_payee_honeypot_fingerprint_match', 'tmx_payee_honeypot_unknown_diff', 'tmx_payee_http_os_signature', 'tmx_payee_http_referer', 'tmx_payee_http_referer_domain', 'tmx_payee_http_referer_domain_assert_history', 'tmx_payee_http_referer_domain_first_seen', 'tmx_payee_http_referer_domain_last_event', 'tmx_payee_http_referer_domain_last_update', 'tmx_payee_http_referer_domain_result', 'tmx_payee_http_referer_domain_score', 'tmx_payee_http_referer_domain_worst_score', 'tmx_payee_http_referer_url', 'tmx_payee_image_anomaly', 'tmx_payee_image_loaded', 'tmx_payee_js_browser_string', 'tmx_payee_js_fonts_hash', 'tmx_payee_js_fonts_number', 'tmx_payee_js_href_domain', 'tmx_payee_mime_type_hash', 'tmx_payee_mime_type_number', 'tmx_payee_multiple_session_id', 'tmx_payee_org_id', 'tmx_payee_os', 'tmx_payee_os_anomaly', 'tmx_payee_os_fonts_hash', 'tmx_payee_os_fonts_number', 'tmx_payee_page_time_on', 'tmx_payee_plugin_adobe_acrobat', 'tmx_payee_plugin_devalvr', 'tmx_payee_plugin_flash', 'tmx_payee_plugin_hash', 'tmx_payee_plugin_java', 'tmx_payee_plugin_number', 'tmx_payee_plugin_quicktime', 'tmx_payee_plugin_realplayer', 'tmx_payee_plugin_shockwave', 'tmx_payee_plugin_silverlight', 'tmx_payee_plugin_svg_viewer', 'tmx_payee_plugin_vlc_player', 'tmx_payee_plugin_windows_media_player', 'tmx_payee_policy', 'tmx_payee_policy_score', 'tmx_payee_profiled_domain', 'tmx_payee_profiled_domain_first_seen', 'tmx_payee_profiled_domain_last_event', 'tmx_payee_profiled_domain_last_update', 'tmx_payee_profiled_domain_result', 'tmx_payee_profiled_domain_score', 'tmx_payee_profiled_domain_worst_score', 'tmx_payee_profiled_url', 'tmx_payee_profiling_datetime', 'tmx_payee_proxy_ip', 'tmx_payee_proxy_ip_activities', 'tmx_payee_proxy_ip_assert_history', 'tmx_payee_proxy_ip_attributes', 'tmx_payee_proxy_ip_city', 'tmx_payee_proxy_ip_first_seen', 'tmx_payee_proxy_ip_geo', 'tmx_payee_proxy_ip_isp', 'tmx_payee_proxy_ip_last_assertion', 'tmx_payee_proxy_ip_last_event', 'tmx_payee_proxy_ip_last_update', 'tmx_payee_proxy_ip_latitude', 'tmx_payee_proxy_ip_longitude', 'tmx_payee_proxy_ip_organization', 'tmx_payee_proxy_ip_region', 'tmx_payee_proxy_ip_result', 'tmx_payee_proxy_ip_score', 'tmx_payee_proxy_ip_worst_score', 'tmx_payee_proxy_name', 'tmx_payee_proxy_type', 'tmx_payee_reason_code', 'tmx_payee_request_duration', 'tmx_payee_request_id', 'tmx_payee_request_result', 'tmx_payee_review_status', 'tmx_payee_risk_rating', 'tmx_payee_screen_aspect_ratio_anomaly', 'tmx_payee_screen_color_depth', 'tmx_payee_screen_dpi', 'tmx_payee_screen_res', 'tmx_payee_screen_res_anomaly', 'tmx_payee_service_type', 'tmx_payee_session_anomaly', 'tmx_payee_session_id', 'tmx_payee_summary_risk_score', 'tmx_payee_system_state', 'tmx_payee_tcp_os_signature', 'tmx_payee_timezone_offset_anomaly', 'tmx_payee_time_zone', 'tmx_payee_time_zone_dst_offset', 'tmx_payee_tmx_policy_score', 'tmx_payee_tmx_reason_code', 'tmx_payee_tmx_review_status', 'tmx_payee_tmx_risk_rating', 'tmx_payee_tmx_summary_reason_code', 'tmx_payee_transaction_amount', 'tmx_payee_transaction_currency', 'tmx_payee_transaction_id', 'tmx_payee_true_ip', 'tmx_payee_true_ip_activities', 'tmx_payee_true_ip_assert_history', 'tmx_payee_true_ip_attributes', 'tmx_payee_true_ip_city', 'tmx_payee_true_ip_first_seen', 'tmx_payee_true_ip_geo', 'tmx_payee_true_ip_isp', 'tmx_payee_true_ip_last_assertion', 'tmx_payee_true_ip_last_event', 'tmx_payee_true_ip_last_update', 'tmx_payee_true_ip_latitude', 'tmx_payee_true_ip_longitude', 'tmx_payee_true_ip_organization', 'tmx_payee_true_ip_region', 'tmx_payee_true_ip_result', 'tmx_payee_true_ip_score', 'tmx_payee_true_ip_worst_score', 'tmx_payee_ua_browser', 'tmx_payee_ua_mobile', 'tmx_payee_ua_os', 'tmx_payee_ua_platform', 'tmx_payee_unknown_session', 'tmx_payee_url_anomaly', 'tmx_payee_rs_ind_No reject status on Email - global - month', 'tmx_payee_rs_ind_No reject status on Exact ID - global - month', 'tmx_payee_rs_ind_New Exact ID', 'tmx_payee_rs_ind_Good Exact ID Age', 'tmx_payee_rs_ind_3IPLogins_inaDay', 'tmx_payee_rs_ind_NoDeviceID', 'tmx_payee_rs_ind_Global Email gt 500 dollars - month', 'tmx_payee_rs_ind_Global Exact ID gt 500 dollars - month', 'tmx_payee_rs_ind_3DifferentDeviceIDs_SameAccountEmailID_inaWeek', 'tmx_payee_rs_ind_3DifferentDeviceIDs_SameAccountLogin_inaWeek', 'tmx_payee_rs_ind_3_Emails_per_device', 'tmx_payee_rs_ind_Review Status', 'tmx_payee_rs_ind_Lang Mismatch', 'tmx_payee_rs_ind_Good global persona - month', 'tmx_payee_rs_ind_AccountAddress_Differentfrom_TrueGeo', 'tmx_payee_rs_ind_global email used mlt places -day', 'tmx_payee_rs_ind_ProxyIP_isHidden', 'tmx_payee_rs_ind_ProxyIPAddress_Risky_inReputationNetwork', 'tmx_payee_rs_ind_3DifferentAccountEmailIDs_SameDeviceID_inaDay', 'tmx_payee_rs_ind_3DifferentAccountLogins_SameDeviceID_inaDay', 'tmx_payee_rs_ind_3DifferentAccountEmailIDs_SameDeviceID_inaWeek', 'tmx_payee_rs_ind_3DifferentAccountLogins_SameDeviceID_inaWeek', 'tmx_payee_rs_ind_global device using mlt personas - day', 'tmx_payee_rs_ind_DeviceTrueGEO_Differentfrom_ProxyGeo', 'tmx_payee_rs_ind_Dial-up connection', 'tmx_payee_rs_ind_2Device_Creation_inanHour', 'tmx_payee_rs_ind_ProxyIP_isAnonymous', 'tmx_payee_rs_ind_3DifferentProxyIPs_SameDeviceID_inaDay', 'tmx_payee_rs_ind_PossibleCookieWiping', 'tmx_payee_rs_ind_3DeviceCreation_inaDay', 'merge_key', 'merge_ind', 'target', 'lo_tmx_payee_tmx_summary_reason_code', 'lo_tmx_payer_proxy_ip', 'lo_tmx_payer_enabled_js', 'lo_tmx_payer_screen_dpi', 'lo_tmx_payee_reason_code', 'lo_signal_355', 'lo_tmx_payer_ua_browser', 'lo_tmx_payee_fuzzy_device_result', 'lo_tmx_payee_account_email_result', 'lo_tmx_payee_proxy_ip_city', 'lo_tmx_payer_proxy_ip_last_event', 'lo_tmx_payee_flash_lang', 'lo_tmx_payee_enabled_im', 'lo_tmx_payee_proxy_ip', 'lo_tmx_payee_plugin_realplayer', 'lo_tmx_payee_plugin_adobe_acrobat', 'lo_signal_13', 'lo_tmx_payer_plugin_java', 'lo_tmx_payee_proxy_ip_result', 'lo_tmx_payer_screen_res', 'lo_signal_506', 'lo_tmx_payee_review_status', 'lo_tmx_payer_plugin_shockwave', 'lo_tmx_payee_transaction_amount', 'lo_tmx_payee_fuzzy_device_match_result', 'lo_tmx_payee_proxy_ip_last_assertion', 'lo_tmx_payee_proxy_type', 'lo_tmx_payer_account_login_result', 'lo_tmx_payer_plugin_windows_media_player', 'lo_tmx_payee_screen_dpi', 'lo_tmx_payer_true_ip_region', 'lo_tmx_payer_http_referer_domain_result', 'lo_tmx_payer_honeypot_fingerprint_check', 'lo_tmx_payee_true_ip_organization', 'lo_tmx_payee_account_address_zip', 'lo_tmx_payee_headers_name_value_hash', 'lo_tmx_payee_enabled_fl', 'lo_tmx_payee_account_address_state', 'lo_tmx_payee_tmx_reason_code', 'lo_tmx_payee_plugin_silverlight', 'lo_tmx_payer_request_result', 'lo_tmx_payee_proxy_ip_first_seen', 'lo_tmx_payer_event_type', 'lo_tmx_payer_device_result', 'lo_tmx_payee_device_assert_history', 'lo_tmx_payer_js_browser_string', 'lo_tmx_payee_request_result', 'lo_tmx_payee_proxy_ip_region', 'lo_tmx_payer_true_ip_geo', 'lo_tmx_payee_agent_type', 'lo_tmx_payer_mime_type_hash', 'lo_tmx_payer_proxy_ip_last_update', 'lo_tmx_payer_honeypot_fingerprint_diff', 'lo_tmx_payer_account_address_country', 'lo_tmx_payer_honeypot_fingerprint_match', 'lo_tmx_payer_ua_platform', 'lo_tmx_payee_js_browser_string', 'lo_tmx_payer_plugin_flash', 'lo_tmx_payee_enabled_ck', 'lo_tmx_payer_honeypot_fingerprint', 'lo_tmx_payee_plugin_shockwave', 'lo_tmx_payee_http_referer_domain', 'lo_tmx_payer_proxy_ip_city', 'lo_tmx_payee_account_login_result', 'lo_signal_600', 'lo_tmx_payee_honeypot_fingerprint_match', 'lo_tmx_payee_mime_type_hash', 'lo_tmx_payer_profiled_domain', 'lo_tmx_payer_http_os_signature', 'lo_tmx_payee_device_result', 'lo_tmx_payer_ua_os', 'lo_tmx_payee_account_address_country', 'lo_tmx_payer_account_email_assert_history', 'lo_tmx_payee_os_fonts_hash', 'lo_tmx_payer_device_match_result', 'lo_tmx_payer_proxy_ip_organization', 'lo_tmx_payer_honeypot_unknown_diff', 'lo_tmx_payer_enabled_ck', 'lo_tmx_payer_true_ip_organization', 'lo_tmx_payer_true_ip_city', 'lo_tmx_payer_device_attributes', 'lo_tmx_payee_honeypot_fingerprint', 'lo_signal_156', 'lo_tmx_payee_http_referer', 'lo_tmx_payee_true_ip_region', 'lo_tmx_payer_account_address_city', 'lo_tmx_payee_risk_rating', 'lo_tmx_payer_proxy_ip_geo', 'lo_tmx_payer_proxy_ip_last_assertion', 'lo_tmx_payee_account_name_result', 'lo_tmx_payee_account_email_assert_history', 'lo_tmx_payee_true_ip_geo', 'lo_signal_8', 'lo_tmx_payee_true_ip', 'lo_tmx_payee_account_address_result', 'lo_tmx_payee_headers_order_string_hash', 'lo_signal_2', 'lo_tmx_payee_ua_platform', 'lo_tmx_payer_fuzzy_device_result', 'lo_tmx_payee_proxy_ip_organization', 'lo_tmx_payer_os', 'lo_tmx_payer_enabled_fl', 'lo_signal_548', 'lo_tmx_payee_honeypot_fingerprint_diff', 'lo_signal_547', 'lo_tmx_payee_screen_res', 'lo_tmx_payee_flash_version', 'lo_tmx_payee_proxy_ip_last_event', 'lo_tmx_payer_plugin_silverlight', 'lo_tmx_payer_fuzzy_device_match_result', 'lo_tmx_payer_agent_type', 'lo_tmx_payee_proxy_ip_last_update', 'lo_tmx_payer_enabled_im', 'lo_tmx_payee_enabled_js', 'lo_tmx_payee_tcp_os_signature', 'lo_tmx_payee_proxy_ip_geo', 'lo_tmx_payee_account_address_city', 'lo_tmx_payer_plugin_adobe_acrobat', 'lo_tmx_payer_account_address_result', 'lo_tmx_payee_honeypot_fingerprint_check', 'lo_tmx_payer_device_assert_history', 'lo_tmx_payee_device_match_result', 'lo_tmx_payer_proxy_ip_result', 'lo_tmx_payer_account_name_result', 'lo_tmx_payer_mime_type_number', 'lo_tmx_payer_plugin_realplayer', 'lo_tmx_payer_headers_order_string_hash', 'lo_tmx_payee_true_ip_result', 'lo_tmx_payee_plugin_hash', 'lo_tmx_payee_os', 'lo_tmx_payee_honeypot_unknown_diff', 'lo_tmx_payer_flash_os', 'lo_tmx_payee_event_type', 'lo_tmx_payee_flash_os', 'lo_tmx_payee_http_referer_domain_result', 'lo_tmx_payee_plugin_windows_media_player', 'lo_tmx_payer_transaction_amount', 'lo_tmx_payee_profiled_domain', 'lo_tmx_payer_http_referer', 'lo_tmx_payer_account_email_attributes', 'lo_tmx_payer_account_email_result', 'lo_tmx_payer_flash_version', 'lo_tmx_payee_http_os_signature', 'lo_tmx_payer_risk_rating', 'lo_tmx_payee_account_email_attributes', 'lo_tmx_payer_http_referer_domain', 'lo_tmx_payer_account_address_state', 'lo_tmx_payee_mime_type_number', 'lo_tmx_payer_proxy_ip_isp', 'lo_signal_166', 'lo_tmx_payer_reason_code', 'lo_tmx_payee_device_attributes', 'lo_tmx_payer_account_address_zip', 'lo_tmx_payer_browser_language', 'lo_tmx_payer_plugin_quicktime', 'lo_tmx_payee_true_ip_isp', 'lo_tmx_payer_true_ip_result', 'lo_tmx_payer_true_ip_isp', 'lo_tmx_payee_true_ip_city', 'lo_tmx_payer_proxy_ip_first_seen', 'lo_tmx_payee_plugin_flash', 'lo_tmx_payee_browser_language', 'lo_tmx_payer_tcp_os_signature', 'lo_tmx_payee_plugin_java', 'lo_tmx_payer_review_status', 'lo_tmx_payer_flash_lang', 'lo_tmx_payee_plugin_quicktime', 'lo_tmx_payee_ua_os', 'lo_signal_177', 'lo_tmx_payee_ua_browser', 'lo_tmx_payee_proxy_ip_isp', 'lo_tmx_payer_proxy_ip_region', 'lo_tmx_payer_proxy_type']
