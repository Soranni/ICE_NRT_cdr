import config

# Inserting values into DB
insert_list_calls = [
"brand_id","call_event","imsi","imei",	"msisdn","call_event_start_timestamp","utc_time_offset","call_event_duration","cause_for_termination",
"bearer_service_code","tele_service_code","supplementary_service_code",
"dialled_digits","connected_number","calling_number","third_party_number","access_point_name_ni","access_point_name_oi","data_volume_incoming",
"data_volume_outgoing","sgsn_address","ggsn_address","rec_entity_id","call_reference","charging_id","charge_amount","zone_to_id","zone_from_id","price",
"call_rated","country_code","plmn","nrtrde_file_id","inserted","modified"
]

insert_list_imported = [
"call_event","imsi","imei",	"call_event_start_timestamp","utc_time_offset","call_event_duration","cause_for_termination",
"bearer_service_code","tele_service_code","supplementary_service_code",
"dialled_digits","connected_number","calling_number","third_party_number","access_point_name_ni","access_point_name_oi","data_volume_incoming",
"data_volume_outgoing","sgsn_address","ggsn_address","rec_entity_id","call_reference","charging_id","charge_amount",
"plmn","nrtrde_file_id","inserted"
]

insert_list_discarded = [
"call_event","imsi","imei",	"call_event_start_timestamp","utc_time_offset","call_event_duration","cause_for_termination",
"bearer_service_code","tele_service_code","supplementary_service_code",
"dialled_digits","connected_number","calling_number","third_party_number","access_point_name_ni","access_point_name_oi","data_volume_incoming",
"data_volume_outgoing","sgsn_address","ggsn_address","rec_entity_id","call_reference","charging_id","charge_amount",
"plmn","nrtrde_file_id","inserted","error_description"
]
def write_to_db(cdrIiterator):
    
    if len(cdrIiterator.bulk_lines) == 0:
        return
    
    db_values_calls = []
    db_values_imported = []
    db_values_discarded = []
    for cdr_data in cdrIiterator.bulk_lines:
        if cdr_data["TYPE"] == "CALLS":
            db_values_calls.append(cdr_data)
        if cdr_data["TYPE"] == "IMPORTED":
            db_values_imported.append(cdr_data)
        if cdr_data["TYPE"] == "DISCARDED":
            db_values_discarded.append(cdr_data)
    
    cur = config.conn.cursor()
    
    i = 0
    for calls_val in db_values_calls:
        vals = ["'" + str(calls_val[str_]) + "'" for str_ in insert_list_calls]
        insert_str = "INSERT INTO calls(" + ", ".join(insert_list_calls ) + ") VALUES(" +  ",".join(vals ) + "); "
        cur.execute(insert_str)
        i+=1
    
    i2 = 0
    for calls_val in db_values_imported:
        vals = ["'" + str(calls_val[str_]) + "'" for str_ in insert_list_imported]
        insert_str = "INSERT INTO imported_calls(" + ", ".join(insert_list_imported ) + ") VALUES(" +  ",".join(vals ) + "); "
        cur.execute(insert_str)
        i2+=1
        
    i3 = 0
    for calls_val in db_values_discarded:
        vals = ["'" + str(calls_val[str_]) + "'" for str_ in insert_list_discarded]
        insert_str = "INSERT INTO discarded_calls(" + ", ".join(insert_list_discarded ) + ") VALUES(" +  ",".join(vals ) + "); "
        cur.execute(insert_str)
        i3+=1
    print(i,i2,i3)  
    config.conn.commit()
    cur.close()
        
    
    #print(db_values_discarded)
    #print(db_values_imported)
    
    """
    with open("TEST1_48.csv", 'w') as f:
    #with open("TEST1_37.csv", 'w') as f:
        for i in db_values:
            for j in i:
                f.write("%s;" %(j))
            f.write('\n')
    """
    #cur.fast_executemany = True
    #cur.executemany(insert_str,db_values)
    #cur.commit()  
    #cur.close()
    
    cdrIiterator.bulk_lines = []
