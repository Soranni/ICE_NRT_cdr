import config

# Inserting values into DB
insert_list_calls = [	
"brand_id",	
"call_event",	
"imsi",	
"imei",	
"msisdn",
"call_event_start_timestamp",
"utc_time_offset",
"call_event_duration",
"cause_for_termination",
"bearer_service_code",
"tele_service_code",
"supplementary_service_code",
"dialled_digits",
"connected_number",
"calling_number",
"third_party_number",
"access_point_name_ni",
"access_point_name_oi",
"data_volume_incoming",
"data_volume_outgoing",
"sgsn_address",
"ggsn_address",
"rec_entity_id",
"call_reference",
"charging_id",
"charge_amount",
"zone_to_id",
"zone_from_id",
"price",
"call_rated",
"country_code",
"plmn",
"nrtrde_file_id",
"inserted",
"modified"
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
    
    for i in db_values_calls:
        print("------------------------------------------------------")
        insert_str = "INSERT INTO [ice_net$Buffer Table - Misc](" + ", ".join(insert_list_calls) + ") VALUES(" + ("? ," * (len(insert_list_calls) - 1)) + "?); "
        print(i)
        print(insert_str)
        
    
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
    #cur = config.conn.cursor()
    #cur.fast_executemany = True
    #cur.executemany(insert_str,db_values)
    #cur.commit()  
    #cur.close()
    
    cdrIiterator.bulk_lines = []
