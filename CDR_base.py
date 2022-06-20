from file_iterator import *
import datetime
"""
All the variables wil be converted to string, int or datetime
either if they are empty or not
This will make it easier for the mapping to SQL
"""

class NRTRecord(CDRiterator):
    def __init__(self):
        super().__init__(the_infix="NRTRECORD", skip_lines=6)
        self.inflix = "NRTRECORD"

    def handle_columns(self, columns):
        if(columns[2] == "MOC" and columns[6] not in ["0",""]):
            self.payload["TYPE"] = "CALLS"
        elif(columns[2] == "MOC" and columns[6] == "0" and self.Hex_to_Text(columns[9]) == "11"):
            self.payload["TYPE"] = "DISCARDED"
        else:
            self.payload["TYPE"] = "IMPORTED"
        
        self.payload["call_event"] = str(columns[2])
        self.payload["imsi"] = str(columns[3].replace("F",""))	
        self.payload["imei"] = str(columns[4].replace("F",""))		
        self.payload["call_event_start_timestamp"] = self.get_date(columns[1], "%Y%m%d%H%M%S", "FullDate") if(columns[1])  else self.get_date("17540101000000", "%Y%m%d%H%M%S", "Date")
        self.payload["utc_time_offset"] = str(self.Hex_to_Text(columns[5]))
        self.payload["call_event_duration"] = int(columns[6]) if columns[6].isnumeric() else int("0")
        self.payload["cause_for_termination"] = int(columns[7]) if columns[7].isnumeric() else int("0")
        self.payload["bearer_service_code"] = str(self.Hex_to_Text(columns[8]))
        self.payload["tele_service_code"] = str(self.Hex_to_Text(columns[9]))
        self.payload["supplementary_service_code"] = str(columns[10])
        self.payload["dialled_digits"] = str(self.Hex_to_Text(columns[11]))
        self.payload["connected_number"] = str(self.Hex_to_Text(columns[12]))
        self.payload["calling_number"] = str(self.Hex_to_Text(columns[13]))
        self.payload["third_party_number"] = str(self.Hex_to_Text(columns[14]))
        self.payload["access_point_name_ni"] = str(self.Hex_to_Text(columns[15]))
        self.payload["access_point_name_oi"] = str(self.Hex_to_Text(columns[16]))
        self.payload["data_volume_incoming"] = int(columns[17]) if columns[17].isnumeric() else int("0")
        self.payload["data_volume_outgoing"] = int(columns[18]) if columns[18].isnumeric() else int("0")
        self.payload["sgsn_address"] = str(self.Hex_to_Text(columns[19]))
        self.payload["ggsn_address"] = str(self.Hex_to_Text(columns[20]))
        self.payload["rec_entity_id"] = str(self.Hex_to_Text(columns[21]))
        self.payload["call_reference"] = int(columns[22]) if columns[22].isnumeric() else int("0")
        self.payload["charging_id"] = int(columns[23]) if columns[23].isnumeric() else int("0")
        self.payload["charge_amount"] = int(columns[24]) if columns[24].isnumeric() else int("0")
        self.payload["plmn"] = str(self.Hex_to_Text(columns[25]))
        self.payload["inserted"] = datetime.datetime.now()
        self.payload["error_description"] = str("")
        
    
        if(self.payload["call_reference"] == 281716207615):
            print(self.payload["TYPE"],":",columns[6],":")
            
        return True